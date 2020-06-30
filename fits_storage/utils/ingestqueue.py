"""
This module provides various utility functions to
manage and service the ingestqueue
"""
import os
import datetime
from sqlalchemy import desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import ObjectDeletedError, NoResultFound
from sqlalchemy.orm import make_transient
from time import sleep
import functools
import fcntl
import dateutil.parser
import sys
import traceback

from fits_storage.orm.provenance import ingest_provenance
from ..orm.geometryhacks import add_footprint, do_std_obs

from ..fits_storage_config import storage_root, using_sqlite, using_s3, using_previews, defer_seconds, use_as_archive
from . import queue

if using_previews:
    from .previewqueue import PreviewQueueUtil

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.diskfilereport import DiskFileReport
from ..orm.fulltextheader import FullTextHeader
from ..orm.header import Header
from ..orm.footprint import Footprint
from ..orm.gmos import Gmos
from ..orm.gnirs import Gnirs
from ..orm.niri import Niri
from ..orm.nifs import Nifs
from ..orm.michelle import Michelle
from ..orm.f2 import F2
from ..orm.gsaoi import Gsaoi
from ..orm.nici import Nici
from ..orm.gpi import Gpi
from ..orm.ingestqueue import IngestQueue
from ..orm.obslog import Obslog
from ..orm.calcachequeue import CalCacheQueue
from ..orm.miscfile import is_miscfile, miscfile_meta, MiscFile

import astrodata
import gemini_instruments

if using_s3:
    from .aws_s3 import get_helper

class IngestError(Exception):
    pass

instrument_table = {
    # Instrument: (Name for debugging, Class)
    'F2':       ("F2", F2),
    'GMOS-N':   ("GMOS", Gmos),
    'GMOS-S':   ("GMOS", Gmos),
    'GNIRS':    ("GNIRS", Gnirs),
    'GPI':      ("GPI", Gpi),
    'GSAOI':    ("GSAOI", Gsaoi),
    'michelle': ("MICHELLE", Michelle),
    'NICI':     ("NICI", Nici),
    'NIFS':     ("NIFS", Nifs),
    'NIRI':     ("NIRI", Niri),
    }

class IngestQueueUtil(object):
    def __init__(self, session, logger, skip_md=True, skip_fv=True, make_previews=False):
        """
        skip_fv: causes the ingest to skip running fitsverify on the files
        skip_md: causes the ingest to skip running md on the files.
        make_preview: If we are doing previews, we usually simply add it to the preview
                      queue. However for a rebuild, it's more efficient to just make the
                      preview when ingesting, while we have the file uncompressed etc.
                      Set this to true to make the preview at ingest time.
        """
        self.s = session
        self.l = logger
        self.skip_md = skip_md
        self.skip_fv = skip_fv
        self.make_previews = make_previews
        if using_previews:
            self.preview = PreviewQueueUtil(self.s, self.l)
        if using_s3:
            self.s3 = get_helper()

    def add_to_queue(self, filename, path, force_md5=False, force=False, after=None):
        """
        Adds a file to the ingest queue.

        Upon success, returns a transient object representing the queue entry. Otherwise,
        it returns None.
        """
        iq = IngestQueue(filename, path)
        iq.force = force
        iq.force_md5 = force_md5
        if after is not None:
            iq.after = after

        self.s.add(iq)
        try:
            self.s.commit()
        except IntegrityError:
            self.l.debug("File %s seems to be in the queue", iq.filename)
            self.s.rollback()
        else:
            make_transient(iq)
            self.l.debug("Added id %s for filename %s to ingestqueue", iq.id, iq.filename)

            return iq

        # Now that it's a transient object, it should not do a lookup so this should never fail
        #try:
            #logger.debug("Added id %d for filename %s to ingestqueue", iq.id, iq.filename)
        #except ObjectDeletedError:
            #logger.debug("Added filename %s to ingestqueue which was immediately deleted", filename)

    def need_to_add_diskfile(self, fileobj, force, force_md5):
        # See if a diskfile for this file already exists and is present
        query = self.s.query(DiskFile)\
                    .filter(DiskFile.file_id == fileobj.id)\
                    .filter(DiskFile.present == True)

        result = False

        try:
            # Assume that the file is there (will raise an exception otherwise)
            diskfile = query.one()
            # Yes, it's already there.
            self.l.debug("already present in diskfile table...")
            # Ensure there's only one and get an instance of it

            def need_to_add_diskfile_p(md5, msg1, msg2):
                # If md5 remains the same, we're good (unless we're forcing it)
                if diskfile.file_md5 == md5 and not force:
                    self.l.debug("{0} indicates no change".format(msg1))
                    return False
                else:
                    self.l.debug("{0} indicates file has changed - reingesting".format(msg2))
                    # We could fetch the file and do a local md5 check here if we want
                    # Set the present and canonical flags on the current one to false and create a new entry
                    diskfile.present = False
                    diskfile.canonical = False
                    self.s.commit()
                    return True

            # Has the file changed since we last ingested it?
            if using_s3:
                # Lastmod on s3 is always the upload time, no way to set it manually
                result = need_to_add_diskfile_p(self.s3.get_md5(diskfile.filename), "S3 etag md5", "S3 etag md5 or force flag")
            else:
                # By default check lastmod time first
                # there is a subtelty wrt timezones here.
                if (diskfile.lastmod.replace(tzinfo=None) != diskfile.get_lastmod()) or force_md5 or force:
                    self.l.debug("lastmod time or force flags suggest file modification")
                    result = need_to_add_diskfile_p(diskfile.get_file_md5(), "md5", "md5/force flag")
                else:
                    self.l.debug("lastmod time indicates file unchanged, not checking further")

        except NoResultFound:
            # No not present, insert into diskfile table
            self.l.debug("No Present DiskFile exists")
            result = True

            # Check to see if there is are older non-present but canonical versions to mark non-canonical
            olddiskfiles = self.s.query(DiskFile)\
                                .filter(DiskFile.canonical == True)\
                                .filter(DiskFile.file_id == fileobj.id)\
                                .filter(DiskFile.present == False)

            for olddiskfile in olddiskfiles:
                self.l.debug("Marking old diskfile id %d as no longer canonical", olddiskfile.id)
                olddiskfile.canonical = False
            self.s.commit()

        return result

    def check_present(self, filename):
        """
        Check to see if the named file is present in the database and
        marked as present in the diskfile table.
        If so, checks to see if it's actually on disk and if not
        marks it as not present in the diskfile table
        """

        # Search for file object
        query = self.s.query(File).filter(File.name == filename)
        try:
            # Assume that there's a file entry for this one
            self.l.debug("%s is present in file table", filename)
            fileobj = query.one()
            # OK, is there a diskfile that's present for it
            query = self.s.query(DiskFile).filter(DiskFile.file_id == fileobj.id).filter(DiskFile.present == True)

            # Assume that there's a diskfile entry for this
            diskfile = query.one()
            self.l.debug("%s is present=True in diskfile table at diskfile_id = %s", filename, diskfile.id)
            # Is the file actually present on disk?
            if diskfile.exists():
                self.l.debug("%s is actually present on disk. That's good", filename)
            else:
                self.l.info("%s is present in diskfile table id %d but missing on the disk.", filename, diskfile.id)
                self.l.info("Marking diskfile id %d as not present", diskfile.id)
                diskfile.present = False
                self.s.commit()
        except NoResultFound:
            pass

    def add_diskfile_entry(self, fileobj, filename, path, fullpath):
        self.l.debug("Adding new DiskFile entry")
        if using_s3:
            # At this point, we fetch a local copy of the file to the staging area
            if not self.s3.fetch_to_staging(filename):
                # Failed to fetch the file from S3. Can't do this
                return

        # Instantiating the DiskFile object with a bzip2 filename will trigger creation of the unzipped cache file too.
        diskfile = DiskFile(fileobj, filename, path)
        try:
            self.s.add(diskfile)
            self.s.commit()
            if diskfile.uncompressed_cache_file:
                self.l.debug("diskfile uncompressed cache file = %s, access=%s", diskfile.uncompressed_cache_file,
                                os.access(diskfile.uncompressed_cache_file, os.F_OK))


            if is_miscfile(filename):
                meta = miscfile_meta(filename)
                misc = MiscFile()
                misc.diskfile_id = diskfile.id
                misc.release = dateutil.parser.parse(meta['release'])
                misc.description = meta['description']
                misc.program_id  = meta['program']

                self.s.add(misc)
                self.s.commit()
            elif 'obslog' in filename:
                obslog = Obslog(diskfile)
                self.s.add(obslog)
                self.s.commit()
            else:
                # Proceed with normal fits file ingestion

                # Instantiate an astrodata object here and pass it in to the things that need it
                # These are expensive to instantiate each time
                if diskfile.uncompressed_cache_file:
                    fullpath_for_ad = diskfile.uncompressed_cache_file
                else:
                    fullpath_for_ad = diskfile.fullpath()

                self.l.debug("Instantiating AstroData object on %s", fullpath_for_ad)
                try:
                    diskfile.ad_object = astrodata.open(fullpath_for_ad)
                except:
                    self.l.error("Failed to open astrodata object on file: %s. Giving up", fullpath_for_ad)

                    self.delete_file(diskfile, fullpath)

                    return

                # This will use the DiskFile unzipped cache file if it exists
                self.l.debug("Adding new DiskFileReport entry")
                dfreport = DiskFileReport(diskfile, self.skip_fv, self.skip_md)
                self.s.add(dfreport)
                self.s.commit()

                self.l.debug("Adding new Header entry")
                # This will use the diskfile ad_object if it exists, else
                # it will use the DiskFile unzipped cache file if it exists
                header = Header(diskfile)
                self.s.add(header)

                ingest_provenance(diskfile)

                inst = header.instrument
                self.s.commit()
                try:
                    fps = header.footprints(diskfile.ad_object)
                    for i in list(fps.keys()):
                        if fps[i] is not None:
                            foot = Footprint(header)
                            foot.populate(i)
                            self.s.add(foot)
                            self.s.commit()
                            add_footprint(self.s, foot.id, fps[i])
                except Exception as e:
                    # self.l.error("Footprint Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
                    pass

                if not using_sqlite:
                    if header.spectroscopy == False:
                        self.l.debug("Imaging - populating PhotStandardObs")
                        do_std_obs(self.s, header.id)

                # This will use the DiskFile unzipped cache file if it exists
                self.l.debug("Adding FullTextHeader entry")
                ftheader = FullTextHeader(diskfile)
                self.s.add(ftheader)
                self.s.commit()
                # Add the instrument specific tables
                # These will use the DiskFile unzipped cache file if it exists
                try:
                    name, instClass = instrument_table[inst]
                    self.l.debug("Adding new {} entry".format(name))
                    entry = instClass(header, diskfile.ad_object)
                    self.s.add(entry)
                    self.s.commit()
                except KeyError:
                    # Unknown instrument. Maybe we should put a message?
                    pass

                # Do the preview here.
                try:
                    if using_previews:
                        self.preview.process(diskfile, make=self.make_previews)
                except:
                    # For debug
                    string = traceback.format_tb(sys.exc_info()[2])
                    string = "".join(string)
                    self.l.error("Error making preview for %s", diskfile.filename)
                    self.l.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)

                # If we are in archive mode and the metadata is OK, add to calcachequeue here
                if use_as_archive and diskfile.mdready:
                    self.l.info("Adding header id %d to calcachequeue", header.id)
                    cq = CalCacheQueue(header.id, sortkey=header.ut_datetime)
                    self.s.add(cq)
                    self.s.commit()

        finally:
            # really really try to clean up the cache file if we have one
            self.delete_file(diskfile, fullpath)

        self.s.commit()

        return True

    def delete_inactive_from_queue(self, filename, busy_wait=0):
        """
        Checks if a file is in the ingest queue.

        If the entry is in there and looks like in progress, and has an associated error, an
        IngestError will be raised.

        If the entry is in progress and busy_wait is > 0, that many seconds will be waited and we'll
        test again. If the entry is still in progress, an IngestError will be raised.

        If the entry is not in progress, it will be removed and `True` will be the result of the
        function. Otherwise, it will return `False`
        """
        try:
            trials = 0
            while trials < 2:
                trials = trials + 1
                obj = self.s.query(IngestQueue).filter(IngestQueue.filename == filename).one()
                if not obj.inprogress:
                    self.s.delete(obj)
                    self.s.commit()
                    return True
                elif obj.error:
                    raise IngestError("The file is stuck in the ingest queue with an error")
                elif (busy_wait > 0 and trials < 2):
                    sleep(busy_wait)

            raise IngestError("The file is stuck for an unreasonable amount of time")
        except NoResultFound:
            # Not present
            return False

    def ingest_file(self, filename, path, force_md5, force):
        """
        Ingests a file into the database. If the file isn't known to the database
        at all, all three (file, diskfile, header) table entries are created.
        If the file is already in the database but has been modified, the
        existing diskfile entry is marked as not present and new diskfile
        and header entries are created. If the file is in the database and
        has not been modified since it was last ingested, then this function
        does not modify the database.

        filename: the filename of the file to ingest
        path: the path to the file to ingest
        force_md5: normally this function will compare the last modified
                             timestamp on the file to that of the record of the file
                             in the database to determine if it has possibly changed,
                             and only checks the md5 if it has possibly changed. Setting
                             this parameter to true forces a md5 comparison irrespective
                             of the last modification timestamps.
        force: causes this function to ingest the file regardless of md5 and
                     modtime.

        return value is a boolean to say whether we added a new diskfile or not
        """

        self.l.debug("ingest_file %s", filename)

        # First, sanity check if the file actually exists
        if using_s3:
            fullpath = os.path.join(storage_root, filename)
            if not self.s3.exists_key(filename):
                self.l.error("cannot access %s in S3 bucket", filename)
                self.check_present(filename)
                return
        else:
            fullpath = os.path.join(storage_root, path, filename)
            exists = os.access(fullpath, os.F_OK | os.R_OK) and os.path.isfile(fullpath)
            if not exists:
                self.l.error("cannot access %s", fullpath)
                self.check_present(filename)
                return

        try:
            # Assume that there exists a file table entry for this
            trimmed_name = File.trim_name(filename)
            fileobj = self.s.query(File).filter(File.name == trimmed_name).one()
            self.l.debug("Already in file table as %s", trimmed_name)
        except NoResultFound:
            # Make a file instance
            fileobj = File(filename)
            self.l.debug("Adding new file table entry")
            self.s.add(fileobj)
            self.s.commit()

        # At this point, 'fileobj' should by a valid DB object.

        if self.need_to_add_diskfile(fileobj, force, force_md5):
            return self.add_diskfile_entry(fileobj, filename, path, fullpath)

        return False

    def delete_file(self, diskfile, fullpath):
        if using_s3:
            self.l.debug("deleting %s from s3_staging_area", os.path.basename(fullpath))
            os.unlink(fullpath)

        if diskfile.uncompressed_cache_file:
            self.l.debug("deleting %s from gz_staging_area", diskfile.uncompressed_cache_file)
            if os.access(diskfile.uncompressed_cache_file, os.F_OK | os.R_OK):
                os.unlink(diskfile.uncompressed_cache_file)
                diskfile.uncompressed_cache_file = None
            else:
                self.l.debug("diskfile claimed to have an diskfile.uncompressed_cache_file, but cannot access it: %s",
                                diskfile.uncompressed_cache_file)

    def length(self):
        return queue.queue_length(IngestQueue, self.s)

    def pop(self, fast_rebuild=False):
        return queue.pop_queue(IngestQueue, self.s, self.l, fast_rebuild)

    def maybe_defer(self, iq):
        """Check if the file was very recently modified or is locked, defer ingestion if it was"""
        after = None

        fullpath = os.path.join(storage_root, iq.path, iq.filename)
        if defer_seconds > 0:
            lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
            now = datetime.datetime.now()
            age = now - lastmod
            defer = datetime.timedelta(seconds=defer_seconds)
            if age < defer:
                self.l.info("Deferring ingestion of recently modified file %s", iq.filename)
                # Defer ingestion of this file for defer_secs
                after = now + defer
        else:
            # Check if it is locked
            try:
                with open(fullpath, "r+") as fd:
                    try:
                        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except IOError:
                        self.l.info("Deferring ingestion of locked file %s", iq.filename)
                        # Defer ingestion of this file for 15 secs
                        after = datetime.datetime.now() + datetime.timedelta(seconds=15)
            except IOError:
                # Probably don't have write permission to the file
                self.l.warning("Could not open %s for update to test lock", fullpath)

        if after is not None:
            # iq is a transient ORM object, find it in the db
            dbiq = self.s.query(IngestQueue).get(iq.id)
            dbiq.after = after
            dbiq.inprogress = False
            self.s.commit()
            return True

        return False

    def set_error(self, trans, exc_type, exc_value, tb):
        "Sets an error message to a transient object"
        queue.add_error(IngestQueue, trans, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        "Deletes a transient object"
        queue.delete_with_id(IngestQueue, trans.id, self.s)
