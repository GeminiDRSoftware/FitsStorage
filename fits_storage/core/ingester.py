"""
This module contains code for ingesting files into the FitsStorage system.
"""
import dateutil.parser

from sqlalchemy import or_
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry


from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.diskfilereport import DiskFileReport
from fits_storage.core.orm.fulltextheader import FullTextHeader
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.footprint import Footprint, footprints
from fits_storage.core import geometryhacks
from fits_storage.cal.instruments import instrument_class

from fits_storage.config import get_config
fsc = get_config()

if fsc.using_s3:
    from fits_storage.server.aws_s3 import get_helper

if fsc.is_archive:
    from fits_storage.server.orm import miscfile

if fsc.is_server:
    from fits_storage.server.orm.obslog import Obslog


class Ingester(object):
    """
    This class provides functionality for ingesting files into the database.
    We instantiate this class once in each serivce_ingest_queue task, and
    then feed it files one at a time by calling ingest_file().
    """
    def __init__(self, session, logger,
                 skip_md=True, skip_fv=True, make_previews=False):
        """
        Instantiate the Ingester class with a session, logger and configuration
        items.

        Parameters
        ----------
        session - database session
        logger - Fits Storage Logger
        skip_md: bool (default False)
            - Whether to skip metadata validation
        skip_fv: bool (default False)
            - Whether to skip fits verification
        make_previews: bool (default False)
            - Whether to build previews during ingestion (True) or to simply
            ass the files to the preview queue (False). Building previews
            during ingestion is more efficient overall as we already have the
            file fetched, open, and uncompressed, but slows down the ingest
            significantly.
        """

        self.s = session
        self.l = logger
        self.skip_md = skip_md
        self.skip_fv = skip_fv
        self.make_previews = make_previews

        # We pull these configuration values into the local namespace for
        # convenience and to allow poking them for testing
        self.storage_root = fsc.storage_root
        self.using_s3 = fsc.using_s3
        self.using_previews = fsc.using_previews
        self.using_sqlite = fsc.using_sqlite
        self.is_archive = fsc.is_archive
        self.export_destinations = fsc.export_destinations

        # If we're using S3, store the S3 helper object here
        if self.using_s3:
            self.s3 = get_helper()

    def ingest_file(self, iqe: IngestQueueEntry):
        """
        Ingests a file into the database. If the file isn't known to the
        database at all, all (file, diskfile, header, etc) table entries are
        created. If the file is already in the database but has been
        modified, the existing diskfile entry is marked as not present and
        new diskfile and header entries are created.

        If the file is in the database and has not been modified since it was
        last ingested, then this function does not modify the database,
        unless iqe.force==True, which will force re-ingestion.

        By default, this function will use the last modified time of the file
        on the filesystem and in the database as a first check of whether a
        file has been modified since last ingested. If the lastmod time has
        changed, then the file md5sum will be calculated to verify if the
        contents have changed. iqe.force_md5==True will force checking the
        file md5sum regardless of the lastmod time.

        ingest_file() should handle everything - including deleting the iqe
        if it successfully ingests it, adding it to the export queue if
        appropriate, and setting the status and error messages in iqe and
        outputting appropriate log messages if there's a failure. We return
        a True / False return status more to make it clear in the code if an
        exit path is a sucess or a failure, but this value does not need to be
        checked or acted on by the caller.

        Parameters
        ----------
        iqe: IngestQueueEntry for file to ingest

        Returns
        -------
        True if we succeeded, False otherwise.
        Note that we do set the iqe status, and commit the session
        if there's a failure.
        """

        self.l.info(f"Considering file for ingest: {iqe.filename}")

        # First, check if the file actually exists. If it doesn't, bail out.
        # Do not attempt to make the file as not present in the database.
        if self.using_s3:
            if not self.s3.exists_key(iqe.filename):
                message = f"Cannot access {iqe.filename} in S3 bucket"
                self.l.error(message)
                iqe.seterror(message)
                self.s.commit()
                return False
        else:
            if not iqe.file_exists:
                message = f"Cannot access {iqe.fullpathfilename}"
                self.l.error(message)
                iqe.seterror(message)
                self.s.commit()
                return False

        # Ensure a File Object exists in the database
        try:
            trimmed_name = File.trim_name(iqe.filename)
            fileobj = self.s.query(File).filter(File.name == trimmed_name).one()
            self.l.debug(f"Already in file table as {trimmed_name}")
        except NoResultFound:
            fileobj = File(iqe.filename)
            self.l.info(f"Adding new file table entry for {iqe.filename}")
            self.s.add(fileobj)
            self.s.commit()

        # At this point, 'fileobj' should by a valid DB  object.

        if self.need_to_add_diskfile(iqe, fileobj) or iqe.force:
            diskfile = self.add_diskfile_entry(fileobj, iqe)
            if diskfile is None:
                # We tried to add a diskfile but failed
                # iqe failed and error status should have been updated
                return False
        else:
            # No need to add a diskfile, we're done. Simply delete this iqe.
            # In this case, we don't delete any failed entries for the same
            # file, as if processing failed after diskfile entry, there could
            # still be value in re-trying those. That will need to be triggered
            # manually by re=adding them with force=True
            self.s.delete(iqe)
            self.s.commit()
            return True

        # At this point, 'diskfile' should be a valid DB object. It could
        # represent a fits file, or an obslog or miscfile etc.

        # If the file is not their type, these add_blah() methods do nothing
        # and return False. If the file is their type, they return True after
        # adding the database entry. They return True even if they fail,
        # in which case they call iqe.seterror() and commit. Note, this
        # if statement construct is a little odd, the actual work is done
        # during evaluation of the condition rather than in the code block
        # for the first two conditions.

        if fsc.is_archive and self.add_miscfile(diskfile, iqe):
            pass
        elif fsc.is_server and self.add_obslog(diskfile, iqe):
            pass
        else:
            # Proceed with normal fits file ingestion
            self.add_fitsfile(diskfile, iqe)

        # We're done with the diskfile we created now
        diskfile.cleanup()

        # If we are exporting to downstream servers, add to export queue now.
        # We don't do this earlier as the downstream server will query back
        # to us about the file, so we need to have ingested it first.
        # This is not supported if we are using s3.
        if self.export_destinations:
            if self.using_s3:
                self.l.error("Export is not supported when using S3")
            else:
                for destination in self.export_destinations:
                    self.l.info(f"Adding {iqe.filename} to exportqueue for "
                                f"destination: {destination}")
                    eqe = ExportQueueEntry(iqe.filename, iqe.path, destination)
                    self.s.add(eqe)
                self.s.commit()

    def need_to_add_diskfile(self, iqe, fileobj):
        """
        Determine whether we need to add a diskfile for this file object and
        ingest queue entry.

        Parameters
        ----------
        fileobj - File instance
        iqe - IngestQueueEntry instance

        Returns
        -------
        True if we do need to add a diskfile entry, False otherwise
        """
        # Does a diskfile for this file already exist that is marked as
        # present? Also check there is not more than one of them.
        query = self.s.query(DiskFile) \
            .filter(DiskFile.file_id == fileobj.id) \
            .filter(DiskFile.present == True)
        try:
            diskfile = query.one()
        except MultipleResultsFound:
            self.l.error("Database Integrity Error - multiple diskfiles marked "
                         f"as present for file id {fileobj.id} "
                         f"name {fileobj.name}. Aborting Ingest")
            return False
        except NoResultFound:
            self.l.debug("No diskfile marked as present found for file id "
                         f"{fileobj.id} name {fileobj.name}. Will add one.")
            return True

        # If we get here, a diskfile does exist that is marked as present.
        # Determine if the file has been updated.

        # First check the lastmod time. This isn't possible on S3
        if not self.using_s3:
            # There is a subtlety regarding timezones here.
            if diskfile.lastmod.replace(tzinfo=None) == \
                    diskfile.get_file_lastmod():
                # lastmod matches and we're not forcing and md5 check
                if iqe.force_md5 is True:
                    self.l.debug("Diskfile lastmod indicates file unchanged, "
                                 "but force_md5 was specified")
                else:
                    self.l.debug("Diskfile lastmod indicates file unchanged.")
                    return False

        # Next, check if the md5 matches.
        if self.using_s3:
            # In S3, we store the md5 in the s3 object metadata
            file_md5 = self.s3.get_md5(diskfile.filename)
        else:
            file_md5 = diskfile.get_file_md5()
        if diskfile.file_md5 == file_md5:
            self.l.debug("MD5s match, file is unchanged")
            return False
        else:
            self.l.debug("MD5s do not match, will reingest")
            self.l.debug(f"Database MD5: {diskfile.file_md5}")
            self.l.debug(f"File MD5: {file_md5}")
            return True

    def add_diskfile_entry(self, fileobj, iqe):
        """
        Adds a diskfile entry to the database. If we succeed, we return
        the new diskfile object after we add it to the database. If we
        fail, we return None, but also we set the status in the iqe object
        and commit the session.
        Parameters
        ----------
        fileobj - The File ORM object that this diskfile references
        iqe - IngestQueueEntry object for new file

        Returns
        -------
        The diskfile object that was added, if it succeeds
        None otherwise
        """

        # First, check to see if there is are older versions that are either
        # present or canonical and mark them as not present and not canonical
        olddiskfiles = self.s.query(DiskFile) \
            .filter(DiskFile.file_id == fileobj.id) \
            .filter(or_(DiskFile.present == True, DiskFile.canonical == True))

        for odf in olddiskfiles:
            self.l.debug(f"Marking old diskfile id {odf.id} as not "
                         f"present and not canonical")
            odf.canonical = False
            odf.present = False
        self.s.commit()

        # Now add the new diskfile

        # If we're ingesting from S3, fetch a local copy now
        if self.using_s3:
            if not self.s3.fetch_to_staging(iqe.filename):
                # Failed to fetch the file from S3.
                message = f"Failed to fetch {iqe.filename} from S3"
                self.l.error(message)
                iqe.seterror(message)
                self.s.commit()
                return None

        self.l.debug("Adding new DiskFile entry for file "
                     f"name {fileobj.name} - id {fileobj.id}")

        # Instantiating the DiskFile object with a bzip2 filename will trigger
        # creation of the unzipped cache file too.
        diskfile = DiskFile(fileobj, iqe.filename, iqe.path, logger=self.l)
        try:
            self.s.add(diskfile)
            self.s.commit()
            return diskfile
        except:
            message = f"Failed to add new diskfile"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()
            return None

    def add_miscfile(self, diskfile, iqe):
        """
        Add a miscfile. This method is a no-op if the diskfile presented
        is not a miscfile. If any errors, set status in iqe.

        Parameters
        ----------
        diskfile - diskfile to add
        iqe - ingestqueueentry

        Returns
        -------
        True if it was a miscfile, False otherwise
        """
        if not miscfile.is_miscfile(diskfile.filename):
            return False

        self.l.debug("This is a Miscfile")
        try:
            meta = miscfile.miscfile_meta(diskfile.filename)
            misc = miscfile.MiscFile()
            misc.diskfile_id = diskfile.id
            misc.release = dateutil.parser.parse(meta['release'])
            # misc.description = meta['description']
            dsc = meta['description']
            if isinstance(dsc, str):
                if dsc.startswith('\\x'):
                    dsc = bytes.fromhex(dsc[2:].replace('\\x', ''))\
                        .decode('utf-8')
            else:
                dsc_bytes = dsc
                dsc = dsc_bytes.decode('utf-8', errors='ignore')
            misc.description = dsc
            misc.program_id = meta['program']

            self.l.info(f"Adding miscfile {diskfile.filename}")
            self.s.add(misc)
            self.s.commit()
        except:
            message = "Error adding miscfile"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()

        return True

    def add_obslog(self, diskfile, iqe):
        """
        Add an obslog. This is a no-op if the file is not an obslog
        Parameters. If any errors, set status in iqe
        ----------
        diskfile - the file to add
        iqe - IngestQueueEntry

        Returns
        -------
        True if it was an obslog, False otherwise
        """
        if 'obslog' not in diskfile.filename:
            return False

        self.l.debug("This is an obslog")
        try:
            obslog = Obslog(diskfile)
            self.s.add(obslog)
            self.s.commit()
        except:
            message = "Error adding obslog"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()

        return True

    def add_fitsfile(self, diskfile, iqe):
        """
        Adds a file to the database using astrodata.
        If any errors, call iqe.seterror(), and commit.
        If the failure was something non-fatal, like failing to add
        fulltextheader, we press on regardless after noting the failure as
        that might still get something useful in to the database.

        Parameters
        ----------
        diskfile - the diskfile to add
        iqe - ingest queue entry

        Returns
        -------
        True if succeeded, False otherwise
        """

        # Note, all these objects use the diskfile ad_object, assuming it is
        # open and ready to use.

        try:
            self.l.debug("Adding new DiskFileReport entry")
            dfreport = DiskFileReport(diskfile, self.skip_fv, self.skip_md)
            self.s.add(dfreport)
            self.s.commit()
        except:
            message = "Error adding DiskFileReport"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()

        try:
            self.l.debug("Adding new FullTextHeader entry")
            ftheader = FullTextHeader(diskfile)
            self.s.add(ftheader)
            self.s.commit()
        except:
            message = "Error adding FullTextHeader"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()

        try:
            self.l.debug("Adding new Header entry")
            header = Header(diskfile, self.l)
            self.s.add(header)
            self.s.commit()
        except:
            message = "Error adding Header"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            return False

        try:
            self.l.debug("Skipping Provenance history - not implemented yet")
            # TODO - implement provenance history calls.
            # ingest_provenance(diskfile)
        except:
            self.l.error("Error adding Provenance history", exc_info=True)
            return False

        try:
            if not self.using_sqlite:
                self.l.debug("Adding Footprints")
                for label, fp in footprints(diskfile.ad_object, self.l).items():
                    # This is a bit quirky because SQLAlchemy doesn't natively
                    # support the geometry types that we use.
                    # Hence, the geometryhacks.py module...
                    footprint = Footprint(header.id, label)
                    self.s.add(footprint)
                    self.s.flush()
                    geometryhacks.add_footprint(self.s, footprint.id, fp)
                self.s.commit()
        except:
            # We don't consider this an ingest failure.
            # Just log the error and press on.
            self.l.error("Error adding Footprints", exc_info=True)

        try:
            if not self.using_sqlite and header.spectroscopy == False:
                self.l.debug("Imaging - populating PhotStandardObs")
                geometryhacks.do_std_obs(self.s, header.id)
        except:
            self.l.error("Error adding populating PhotStandardObs",
                         exc_info=True)
            # We don't consider this an ingest failure
            # Just log the error and press on

        try:
            # Add the instrument specific table entry now
            inst = header.instrument
            instclass = instrument_class.get(inst)
            if instclass is not None:
                self.l.debug(f"Adding new {inst} entry")
                entry = instclass(header, diskfile.ad_object)
                self.s.add(entry)
                self.s.commit()
        except:
            message = "Error adding Instrument Table Entry"
            self.l.error(message, exc_info=True)
            iqe.seterror(message)
            self.s.commit()
            return False

        try:
            # Handle previews here
            if self.using_previews:
                if self.make_previews:
                    self.l.info("Skipping Building Preview. Code needs work.")
                    # TODO - fix this
                else:
                    self.l.info(f"Adding {diskfile.filename} to preview queue")
                    pqe = PreviewQueueEntry(diskfile)
                    self.s.add(pqe)
                    self.s.commit()
        except:
            self.l.error("Error adding Previews", exc_info=True)
            # We don't consider this an ingest failure
            # Just log the error and press on

        try:
            # If we are in archive mode, add to calcachequeue here
            if self.is_archive:
                self.l.info("Adding header id %d to calcachequeue" % header.id)
                ccqe = CalCacheQueueEntry(header.id, diskfile.filename)
                self.s.add(ccqe)
                self.s.commit()
        except:
            self.l.error("Error adding to CalCacheQueue", exc_info=True)
            # We don't consider this an ingest failure
            # Just log the error and press on

        # Yay. If we got here, we successfully ingested the file.
        # Delete any iqe entries for this filename that are marked as failed
        failed_iqes = self.s.query(IngestQueueEntry)\
            .filter(IngestQueueEntry.fail_dt != iqe.fail_dt_false)\
            .filter(IngestQueueEntry.filename == iqe.filename)\
            .filter(IngestQueueEntry.path == iqe.path)

        for failed_iqe in failed_iqes:
            self.s.delete(failed_iqe)

        # And delete the iqe that we're working on...
        self.s.delete(iqe)
        self.s.commit()

        return True