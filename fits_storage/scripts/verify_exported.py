import requests
import shutil

from fits_storage.orm import session_scope
from fits_storage.orm.exportqueue import ExportQueue
from fits_storage.orm.file import File
from fits_storage.orm.diskfile import DiskFile

from fits_storage.logger import logger, setdebug

from sqlalchemy import join, desc
import datetime
from optparse import OptionParser

from fits_storage.fits_storage_config import using_s3, storage_root
from fits_storage.utils.hashes import md5sum
from os.path import basename
from glob import iglob

from fits_storage.utils.ingestqueue import IngestQueueUtil


parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--path", action="store", type="string", dest="path", help="Path within /sci/dataflow")
parser.add_option("--filename-pre", action="store", type="string", dest="filepre", help="Filename prefix to filter on")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


snapshotdir = options.snapshotdir
path = options.path
if path is None:
    path = ""
filepre = options.filepre

# Annouce startup
logger.info("*********    verify_exported.py - starting up at %s" % datetime.datetime.now())
print("*********    verify_exported.py - starting up at %s" % datetime.datetime.now())

if using_s3:
    logger.warning("Not compatible with S3, exiting")
    exit(1)

# Get a database session
with session_scope() as session:
    count = 0
    iq = IngestQueueUtil(session, logger)

    if path:
        filenames = iglob("%s/%s/%s*.fits*" % (storage_root, path, filepre))
    else:
        filenames = iglob("%s/%s*.fits*" % (storage_root, filepre))
    for filename in filenames:
        basefilename = basename(filename)
        # Get a list of all diskfile_ids marked as present
        if path != '':
            query = session.query(DiskFile) \
                .filter(DiskFile.path == path).filter(DiskFile.canonical == True). \
                filter(DiskFile.filename == basefilename).order_by(desc(DiskFile.lastmod))
        else:
            query = session.query(DiskFile) \
                .filter(DiskFile.canonical == True). \
                filter(DiskFile.filename == basefilename).order_by(desc(DiskFile.lastmod))

        record = query.one_or_none()

        if record is not None:
            # This file has been ingested, next see if it is in the export queue
            query = session.query(ExportQueue) \
                .filter(ExportQueue.filename == basefilename)
            export_record = query.one_or_none()
            if export_record is None or export_record.failed:
                # we have to see if this is on Archive
                r = requests.get("https://archive.gemini.edu/jsonfilelist/filepre=%s" % basefilename)
                if r.status_code != 200:
                    logger.error("Unable to check on archive for file %s" % basefilename)
                else:
                    if basefilename not in r.body:
                        # Not found on archive, question now is, did it fail to export or we never tried?
                        if export_record is not None:
                            logger.error("File %s not found on archive and had error in export queue")
                        else:
                            logger.error("File %s not found on archive and not found in export queue")

logger.info("*** verify_exported.py exiting normally at %s" % datetime.datetime.now())
