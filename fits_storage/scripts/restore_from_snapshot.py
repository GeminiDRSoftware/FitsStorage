import shutil

from fits_storage.orm import session_scope
from fits_storage.orm.file import File
from fits_storage.orm.diskfile import DiskFile

from fits_storage.logger import logger, setdebug

from sqlalchemy import join, desc
import datetime
from optparse import OptionParser

from fits_storage.fits_storage_config import using_s3
from fits_storage.utils.hashes import md5sum
from os.path import basename
from glob import iglob

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--snapshotdir", action="store", type="string", dest="snapshotdir", help="Location of snapshot folder (NOT including path)")
parser.add_option("--path", action="store", type="string", dest="path", help="Path within snapshot directory and /sci/dataflow")
parser.add_option("--filename-pre", action="store", type="string", dest="filepre", help="Filename prefix to filter on")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


snapshotdir = options.snapshotdir
path = options.path
filepre = options.filepre

# Annouce startup
logger.info("*********    restore_from_snapshot.py - starting up at %s" % datetime.datetime.now())

if using_s3:
    logger.warning("Not compatible with S3, exiting")
    exit(1)

# Get a database session
with session_scope() as session:
    if path:
        filenames = iglob("%s/%s/%s*.fits*" % (snapshotdir, path, filepre))
    else:
        filenames = iglob("%s/%s*.fits*" % (snapshotdir, filepre))
    for filename in filenames:
        basefilename = basename(filename)
        # Get a list of all diskfile_ids marked as present
        query = session.query(DiskFile) \
            .filter(DiskFile.path == path).filter(DiskFile.canonical == True). \
            filter(DiskFile.filename == basefilename).order_by(desc(DiskFile.lastmod))

        record = query.one_or_none()

        if record is not None:
            if record.exists():
                # nothing to do, file is already on disk
                logger.info("File %s already exists on dataflow, no need to restore", filename)
            else:
                md5s = md5sum(filename)
                if md5s != record.file_md5:
                    logger.info("File %s has mismatched md5, unable to restore in place", filename)
                    print("File %s has mismatched md5, unable to restore in place", filename)
                else:
                    # ok, we want to restore it, make a fresh check that there are no other 'present' records
                    query = session.query(DiskFile) \
                        .filter(DiskFile.present == True).filter(DiskFile.filename == basefilename)
                    present_check = query.one_or_none()
                    if present_check:
                        logger.info("Found a 'present' record in the database, unable to restore in place")
                        print("Found a 'present' record in the database, unable to restore in place")
                    else:
                        if path:
                            dest = '/sci/dataflow/%s/%s' % (path, basename(filename))
                        else:
                            dest = '/sci/dataflow/%s' % basename(filename)
                        if options.debug:
                            logger.info("Would copy from %s to %s and flag as present" % (filename, dest))
                            print("Would copy from %s to %s and flag as present" % (filename, dest))
                        else:
                            logger.info("Restoring %s to %s and flagging as present" % (filename, dest))
                            print("Restoring %s to %s and flagging as present" % (filename, dest))
                            shutil.copy(filename, dest)
                            record.present = True
        else:
            logger.warn("No canonical diskfile found for %s" % filename)
            print("No canonical diskfile found for %s" % filename)

logger.info("*** restore_from_snapshot.py exiting normally at %s" % datetime.datetime.now())
