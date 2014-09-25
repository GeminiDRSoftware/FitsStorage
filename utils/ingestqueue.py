"""
This module provides various utility functions to
manage and service the ingestqueue
"""
import os
import datetime
from logger import logger
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

from orm.geometryhacks import add_footprint, do_std_obs

from fits_storage_config import storage_root, using_sqlite, using_s3

from orm.file import File
from orm.diskfile import DiskFile
from orm.diskfilereport import DiskFileReport
from orm.fulltextheader import FullTextHeader
from orm.header import Header
from orm.footprint import Footprint
from orm.gmos import Gmos
from orm.gnirs import Gnirs
from orm.niri import Niri
from orm.nifs import Nifs
from orm.michelle import Michelle
from orm.f2 import F2
from orm.ingestqueue import IngestQueue

from utils.aws_s3 import get_s3_md5, fetch_to_staging

from astrodata import AstroData

if using_s3:
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

def add_to_ingestqueue(session, filename, path, force_md5=False, force=False, after=None):
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
    try:
        logger.debug("Added id %d for filename %s to ingestqueue", iq.id, iq.filename)
        return iq.id
    except ObjectDeletedError:
        logger.debug("Added filename %s to ingestqueue which was immediately deleted", filename)


def ingest_file(session, filename, path, force_md5, force, skip_fv, skip_wmd):
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
    skip_wmd: causes the ingest to skip running wmd on the file.

    return value is a boolean to say whether we added a new diskfile or not
    """

    logger.debug("ingest_file %s", filename)

    # If we're using S3, get the connection and the bucket
    if using_s3:
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)

    # First, sanity check if the file actually exists
    if using_s3:
        key = bucket.get_key(os.path.join(path, filename))
        fullpath = os.path.join(storage_root, filename)
        if key is None:
            logger.error("cannot access %s in S3 bucket", filename)
            check_present(session, filename)
            return
    else:
        fullpath = os.path.join(storage_root, path, filename)
        exists = os.access(fullpath, os.F_OK | os.R_OK) and os.path.isfile(fullpath)
        if not exists:
            logger.error("cannot access %s", fullpath)
            check_present(session, filename)
            return

    # Make a file instance
    fileobj = File(filename)

    # Check if there is already a file table entry for this.
    # filename may have been trimmed by the file object
    query = session.query(File).filter(File.name == fileobj.name)
    if query.first():
        logger.debug("Already in file table as %s", fileobj.name)
        # This will throw an error if there is more than one entry
        fileobj = query.one()
    else:
        logger.debug("Adding new file table entry")
        session.add(fileobj)
        session.commit()

    # At this point, 'fileobj' should by a valid DB object.

    # See if a diskfile for this file already exists and is present
    query = session.query(DiskFile).filter(DiskFile.file_id == fileobj.id).filter(DiskFile.present == True)
    if query.first():
        # Yes, it's already there.
        logger.debug("already present in diskfile table...")
        # Ensure there's only one and get an instance of it
        diskfile = query.one()

        # Has the file changed since we last ingested it?
        if using_s3:
            # Check the md5 from s3 first.
            # Lastmod on s3 is always the upload time, no way to set it manually
            if diskfile.file_md5 == get_s3_md5(key) and (force != True):
                logger.debug("S3 etag md5 indicates no change")
                add_diskfile = 0
            else:
                logger.debug("S3 etag md5 or force flag indicates file has changed - reingesting")
                # We could fetch the file and do a local md5 check here if we want
                # Set the present and canonical flags on the current one to false and create a new entry
                diskfile.present = False
                diskfile.canonical = False
                session.commit()
                add_diskfile = 1
        else:
            # By default check lastmod time first
            # there is a subelty wrt timezones here.
            if (diskfile.lastmod.replace(tzinfo=None) != diskfile.get_lastmod()) or force_md5 or force:
                logger.debug("lastmod time or force flags indicates file modification")
                # Check the md5 to be sure if it's changed
                if diskfile.file_md5 == diskfile.get_file_md5() and (force != True):
                    logger.debug("md5 indicates no change")
                    add_diskfile = 0
                else:
                    logger.debug("md5/force flag indicates file has changed - reingesting")
                    # Set the present and canonical flags on the current one to false and create a new entry
                    diskfile.present = False
                    diskfile.canonical = False
                    session.commit()
                    add_diskfile = 1
            else:
                logger.debug("lastmod time indicates file unchanged, not checking further")
                add_diskfile = 0

    else:
        # No not present, insert into diskfile table
        logger.debug("No Present DiskFile exists")
        add_diskfile = 1

        # Check to see if there is are older non-present but canonical versions to mark non-canonical
        query = session.query(DiskFile).filter(DiskFile.canonical == True)
        query = query.filter(DiskFile.file_id == fileobj.id).filter(DiskFile.present == False)

        olddiskfiles = query.all()
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

        # Instantiating the DiskFile object with a gzipped filename will trigger creation of the unzipped cache file too.
        diskfile = DiskFile(fileobj, filename, path)
        session.add(diskfile)
        session.commit()
        if diskfile.uncompressed_cache_file:
            logger.debug("diskfile uncompressed cache file = %s, access=%s", diskfile.uncompressed_cache_file,
                            os.access(diskfile.uncompressed_cache_file, os.F_OK))


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
            return

        # This will use the DiskFile unzipped cache file if it exists
        logger.debug("Adding new DiskFileReport entry")
        dfreport = DiskFileReport(diskfile, skip_fv, skip_wmd)
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
        if inst == 'GMOS-N' or inst == 'GMOS-S':
            logger.debug("Adding new GMOS entry")
            gmos = Gmos(header, diskfile.ad_object)
            session.add(gmos)
            session.commit()
        if inst == 'NIRI':
            logger.debug("Adding new NIRI entry")
            niri = Niri(header, diskfile.ad_object)
            session.add(niri)
            session.commit()
        if inst == 'GNIRS':
            logger.debug("Adding new GNIRS entry")
            gnirs = Gnirs(header, diskfile.ad_object)
            session.add(gnirs)
            session.commit()
        if inst == 'NIFS':
            logger.debug("Adding new NIFS entry")
            nifs = Nifs(header, diskfile.ad_object)
            session.add(nifs)
            session.commit()
        if inst == 'F2':
            logger.debug("Assing new F2 entry")
            flam2 = F2(header, diskfile.ad_object)
            session.add(flam2)
            session.commit()
        if inst == 'michelle':
            logger.debug("Adding new MICHELLE entry")
            michelle = Michelle(header, diskfile.ad_object)
            session.add(michelle)
            session.commit()

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


def check_present(session, filename):
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

def pop_ingestqueue(session, fast_rebuild=False):
    """
    Returns the next thing to ingest off the ingest queue, and sets the
    inprogress flag on that entry.

    The ORM instance returned is detached from the database - it's a transient
    object not associated with the session. Basicaly treat it as a convenience
    dictionary for the filename etc, but don't try to modify the database with it.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the ingest queue.

    Next to ingest is defined by a sort on the sortkey, which is
    the filename with the first character dropped off - so we effectively
    sort by date and frame number for raw data files.

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.
    """

    # Is there a way to avoid the ACCESS EXCLUSIVE lock, especially with 
    # fast_rebuild where we are not changing other columns. Seemed like
    # SELECT FOR UPDATE ought to be able to do this, but it doesn't quite
    # do what we want as other threads can still select that row?

    session.execute("LOCK TABLE ingestqueue IN ACCESS EXCLUSIVE MODE;")

    query = session.query(IngestQueue).filter(IngestQueue.inprogress == False)
    query = query.filter(IngestQueue.after < datetime.datetime.now())
    query = query.order_by(desc(IngestQueue.sortkey))

    iq = query.first()
    if iq is None:
        logger.debug("No item to pop on ingestqueue")
    else:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from ingestqueue", iq.id)
        # Set this entry to in progres and flush to the DB.
        iq.inprogress = True
        session.flush()

        if not fast_rebuild:
            # Find other instances and delete them
            others = session.query(IngestQueue)
            others = others.filter(IngestQueue.inprogress == False).filter(IngestQueue.filename == iq.filename)
            others.delete()

        # Make the iq into a transient instance before we return it
        # This detaches it from the session, basically it becomes a convenience container for the
        # values (filename, path, etc). The problem is that if it's still attached to the session
        # but expired (because we did a commit) then the next reference to it will initiate a transaction
        # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the exportqueue table until we complete the export and do a commit - which will prevent
        # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
        make_transient(iq)

    # And we're done, commit the transaction and release the update lock
    session.commit()
    return iq

def ingestqueue_length(session):
    """
    return the length of the ingest queue
    """
    length = session.query(IngestQueue).filter(IngestQueue.inprogress == False).count()
    # Even though there's nothing to commit, close the transaction
    session.commit()
    return length

