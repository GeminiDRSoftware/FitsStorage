"""
This module provides various utility functions to
manage and service the ingestqueue
"""
import os
import datetime
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError, NoResultFound
from sqlalchemy.orm import make_transient
import functools

from ..orm.geometryhacks import add_footprint, do_std_obs

from ..fits_storage_config import storage_root, using_sqlite, using_s3, using_previews
from . import queue

if using_previews:
    from .previewqueue import make_preview

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
from ..orm.previewqueue import PreviewQueue
from ..orm.obslog import Obslog

from astrodata import AstroData

if using_s3:
    from boto.s3.connection import S3Connection
    from .aws_s3 import get_s3_md5, fetch_to_staging
    from ..fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

def add_to_ingestqueue(session, logger, filename, path, force_md5=False, force=False, after=None):
    """
    Adds a file to the ingest queue
    """
    iq = IngestQueue(filename, path)
    if force:
        iq.force = True
    if force_md5:
        iq.force_md5 = True
    if after:
        iq.after = after

    session.add(iq)
    session.commit()
    make_transient(iq)
    logger.debug("Added id %d for filename %s to ingestqueue", iq.id, iq.filename)

    # Now that it's a transient object, it should not do a lookup so this should never fail
    #try:
        #logger.debug("Added id %d for filename %s to ingestqueue", iq.id, iq.filename)
    #except ObjectDeletedError:
        #logger.debug("Added filename %s to ingestqueue which was immediately deleted", filename)

instrument_table = {
    # Instrument: (Name for debugging, Class)
    'F2':       ("F2", F2),
    'GMOS-N':   ("GMOS", Gmos),
    'GMOS-S':   ("GMOS", Gmos),
    'GNIRS':    ("GNIRS", Gnirs),
    'GPI':      ("GPI", Gpi),
    'GSAOI':    ("GSAOI", Gsaoi),
    'michelle': ("MICHELLE", Michelle),
    'NICI':     ("NIFS", Nifs),
    'NIFS':     ("NIFS", Nifs),
    'NIRI':     ("NIRI", Niri),
    }

def ingest_file(session, logger, filename, path, force_md5, force, skip_fv, skip_md, make_previews=False):
    """
    Ingests a file into the database. If the file isn't known to the database
    at all, all three (file, diskfile, header) table entries are created.
    If the file is already in the database but has been modified, the
    existing diskfile entry is marked as not present and new diskfile
    and header entries are created. If the file is in the database and
    has not been modified since it was last ingested, then this function
    does not modify the database.

    session: the sqlalchemy database session to use
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
    skip_fv: causes the ingest to skip running fitsverify on the file
    skip_md: causes the ingest to skip running md on the file.
    make_preview: If we are doing previews, we usually simply add it to the preview
                  queue here. However for a rebuild, it's more efficient to just make
                  the preview at this point while we have the file uncompressed etc.
                  Set this to true to make the preview at ingest time.

    return value is a boolean to say whether we added a new diskfile or not
    """

    logger.debug("ingest_file %s", filename)

    # First, sanity check if the file actually exists
    if using_s3:
        # If we're using S3, get the connection and the bucket
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)

        key = bucket.get_key(os.path.join(path, filename))
        fullpath = os.path.join(storage_root, filename)
        if key is None:
            logger.error("cannot access %s in S3 bucket", filename)
            check_present(session, logger, filename)
            return
    else:
        fullpath = os.path.join(storage_root, path, filename)
        exists = os.access(fullpath, os.F_OK | os.R_OK) and os.path.isfile(fullpath)
        if not exists:
            logger.error("cannot access %s", fullpath)
            check_present(session, logger, filename)
            return

    try:
        # Assume that there exists a file table entry for this
        trimmed_name = File.trim_name(filename)
        fileobj = session.query(File).filter(File.name == trimmed_name).one()
        logger.debug("Already in file table as %s", trimmed_name)
    except NoResultFound:
        # Make a file instance
        fileobj = File(filename)
        logger.debug("Adding new file table entry")
        session.add(fileobj)
        session.commit()

    # At this point, 'fileobj' should by a valid DB object.

    # See if a diskfile for this file already exists and is present
    query = session.query(DiskFile)\
                .filter(DiskFile.file_id == fileobj.id)\
                .filter(DiskFile.present == True)

    try:
        # Assume that the file is there (will raise an exception otherwise)
        diskfile = query.one()
        add_diskfile = False
        # Yes, it's already there.
        logger.debug("already present in diskfile table...")
        # Ensure there's only one and get an instance of it

        def need_to_add_diskfile_p(md5, msg1, msg2):
            # If md5 remains the same, we're good (unless we're forcing it)
            if diskfile.file_md5 == md5 and not force:
                logger.debug("{0} indicates no change".format(msg1))
                return False
            else:
                logger.debug("{0} indicates file has changed - reingesting".format(msg2))
                # We could fetch the file and do a local md5 check here if we want
                # Set the present and canonical flags on the current one to false and create a new entry
                diskfile.present = False
                diskfile.canonical = False
                session.commit()
                return True

        # Has the file changed since we last ingested it?
        if using_s3:
            # Lastmod on s3 is always the upload time, no way to set it manually
            add_diskfile = need_to_add_diskfile_p(get_s3_md5(key), "S3 etag md5", "S3 etag md5 or force flag")
        else:
            # By default check lastmod time first
            # there is a subtelty wrt timezones here.
            if (diskfile.lastmod.replace(tzinfo=None) != diskfile.get_lastmod()) or force_md5 or force:
                logger.debug("lastmod time or force flags suggest file modification")
                add_diskfile = need_to_add_diskfile_p(diskfile.get_file_md5(), "md5", "md5/force flag")
            else:
                logger.debug("lastmod time indicates file unchanged, not checking further")

    except NoResultFound:
        # No not present, insert into diskfile table
        logger.debug("No Present DiskFile exists")
        add_diskfile = True

        # Check to see if there is are older non-present but canonical versions to mark non-canonical
        olddiskfiles = session.query(DiskFile)\
                            .filter(DiskFile.canonical == True)\
                            .filter(DiskFile.file_id == fileobj.id)\
                            .filter(DiskFile.present == False)

        for olddiskfile in olddiskfiles:
            logger.debug("Marking old diskfile id %d as no longer canonical", olddiskfile.id)
            olddiskfile.canonical = False
        session.commit()

    if add_diskfile:
        logger.debug("Adding new DiskFile entry")
        if using_s3:
            # At this point, we fetch a local copy of the file to the staging area
            fetched = fetch_to_staging(bucket, path, filename, key, fullpath)
            if not fetched:
                # Failed to fetch the file from S3. Can't do this
                return

        # Instantiating the DiskFile object with a bzip2 filename will trigger creation of the unzipped cache file too.
        diskfile = DiskFile(fileobj, filename, path)
        session.add(diskfile)
        session.commit()
        if diskfile.uncompressed_cache_file:
            logger.debug("diskfile uncompressed cache file = %s, access=%s", diskfile.uncompressed_cache_file,
                            os.access(diskfile.uncompressed_cache_file, os.F_OK))


        # If it's an obslog file, process it as such 
        if 'obslog' in filename:
            obslog = Obslog(diskfile)
            session.add(obslog)
            session.commit()
        else:
            # Proceed with normal fits file ingestion

            # Instantiate an astrodata object here and pass it in to the things that need it
            # These are expensive to instantiate each time
            if diskfile.uncompressed_cache_file:
                fullpath_for_ad = diskfile.uncompressed_cache_file
            else:
                fullpath_for_ad = diskfile.fullpath()

            logger.debug("Instantiating AstroData object on %s", fullpath_for_ad)
            try:
                diskfile.ad_object = AstroData(fullpath_for_ad, mode='readonly')
            except:
                logger.error("Failed to open astrodata object on file: %s. Giving up", fullpath_for_ad)
                if diskfile.ad_object:
                    logger.debug("Closing centrally opened astrodata object")
                    diskfile.ad_object.close()

                if using_s3:
                    logger.debug("deleting %s from s3_staging_area", filename)
                    os.unlink(fullpath)
                if diskfile.uncompressed_cache_file:
                    logger.debug("deleting %s from gz_staging_area", diskfile.uncompressed_cache_file)
                    if os.access(diskfile.uncompressed_cache_file, os.F_OK | os.R_OK):
                        os.unlink(diskfile.uncompressed_cache_file)
                        diskfile.uncompressed_cache_file = None
                    else:
                        logger.debug("diskfile claimed to have an diskfile.uncompressed_cache_file, but cannot access it: %s",
                                        diskfile.uncompressed_cache_file)

                return

            # This will use the DiskFile unzipped cache file if it exists
            logger.debug("Adding new DiskFileReport entry")
            dfreport = DiskFileReport(diskfile, skip_fv, skip_md)
            session.add(dfreport)
            session.commit()

            logger.debug("Adding new Header entry")
            # This will use the diskfile ad_object if it exists, else
            # it will use the DiskFile unzipped cache file if it exists
            header = Header(diskfile)
            session.add(header)
            inst = header.instrument
            logger.debug("Instrument is: %s", inst)
            session.commit()
            logger.debug("Adding new Footprint entries")
            try:
                fps = header.footprints(diskfile.ad_object)
                for i in fps.keys():
                    foot = Footprint(header)
                    foot.populate(i)
                    session.add(foot)
                    session.commit()
                    add_footprint(session, foot.id, fps[i])
            except:
                pass

            if not using_sqlite:
                if header.spectroscopy == False:
                    logger.debug("Imaging - populating PhotStandardObs")
                    do_std_obs(session, header.id)

            # This will use the DiskFile unzipped cache file if it exists
            logger.debug("Adding FullTextHeader entry")
            ftheader = FullTextHeader(diskfile)
            session.add(ftheader)
            session.commit()
            # Add the instrument specific tables
            # These will use the DiskFile unzipped cache file if it exists
            try:
                name, instClass = instrument_table[inst]
                logger.debug("Adding new {} entry".format(name))
                entry = instClass(header, diskfile.ad_object)
                session.add(entry)
                session.commit()
            except KeyError:
                # Unknown instrument. Maybe we should put a message?
                pass

            # Do the preview here.
            try:
                if using_previews:
                    if make_previews:
                        # Go ahead and make the preview now
                        logger.debug("Making Preview")
                        make_preview(session, logger, diskfile)
                    else:
                        # Add it to the preview queue
                        logger.debug("Adding to preview queue")
                        pq = PreviewQueue(diskfile)
                        session.add(pq)
                    session.commit()
            except:
                logger.error("Error making preview for %s", diskfile.filename)

        if diskfile.ad_object:
            logger.debug("Closing centrally opened astrodata object")
            diskfile.ad_object.close()

        if using_s3:
            logger.debug("deleting %s from s3_staging_area", filename)
            os.unlink(fullpath)
        if diskfile.uncompressed_cache_file:
            logger.debug("deleting %s from gz_staging_area", diskfile.uncompressed_cache_file)
            if os.access(diskfile.uncompressed_cache_file, os.F_OK | os.R_OK):
                os.unlink(diskfile.uncompressed_cache_file)
                diskfile.uncompressed_cache_file = None
            else:
                logger.debug("diskfile claimed to have an diskfile.uncompressed_cache_file, but cannot access it: %s",
                                diskfile.uncompressed_cache_file)

    session.commit()

    return add_diskfile


def check_present(session, logger, filename):
    """
    Check to see if the named file is present in the database and
    marked as present in the diskfile table.
    If so, checks to see if it's actually on disk and if not
    marks it as not present in the diskfile table
    """

    # Search for file object
    query = session.query(File).filter(File.name == filename)
    if query.first():
        logger.debug("%s is present in file table", filename)
        fileobj = query.one()
        # OK, is there a diskfile that's present for it
        query = session.query(DiskFile).filter(DiskFile.file_id == fileobj.id).filter(DiskFile.present == True)
        if query.first():
            diskfile = query.one()
            logger.debug("%s is present=True in diskfile table at diskfile_id = %s", filename, diskfile.id)
            # Is the file actually present on disk?
            if fileobj.exists():
                logger.debug("%s is actually present on disk. That's good", filename)
            else:
                logger.info("%s is present in diskfile table id %d but missing on the disk.", filename, diskfile.id)
                logger.info("Marking diskfile id %d as not present", diskfile.id)
                diskfile.present = False

pop_ingestqueue = functools.partial(queue.pop_queue, IngestQueue)

ingestqueue_length = functools.partial(queue.queue_length, IngestQueue)
