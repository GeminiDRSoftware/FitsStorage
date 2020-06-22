import requests

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


parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--path", action="store", type="string", dest="path", help="Path within /sci/dataflow")
parser.add_option("--filename-pre", action="store", type="string", dest="filepre", help="Filename prefix to filter on")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


path = options.path
if path is None:
    path = ""
filepre = options.filepre
if not filepre:
    print("--filename-pre is required, we use it to scan the archive contents efficiently")
if len(filepre) < 6:
    print("Minimum 6 character filename-pre length")

# Annouce startup
logger.info("*********    verify_exported.py - starting up at %s" % datetime.datetime.now())
print("*********    verify_exported.py - starting up at %s" % datetime.datetime.now())

if using_s3:
    logger.warning("Not compatible with S3, exiting")
    exit(1)

# Get a database session
with session_scope() as session:
    count = 0

    if path:
        filenames = iglob("%s/%s/%s*.fits*" % (storage_root, path, filepre))
    else:
        filenames = iglob("%s/%s*.fits*" % (storage_root, filepre))

    r = None # for later, we query archive

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

        record = query.first()

        if record is not None:
            # This file has been ingested, next see if it is in the export queue
            query = session.query(ExportQueue) \
                .filter(ExportQueue.filename == basefilename)
            export_record = query.first()
            if export_record is None or export_record.failed:
                # we have to see if this is on Archive
                if r is None:
                    r = requests.get("https://archive.gemini.edu/jsonfilelist/filepre=%s" % filepre)
                    if r.status_code != 200:
                        logger.error("Unable to check on archive for file %s" % basefilename)
                        exit(1)
                if basefilename not in r.text:
                    # Not found on archive, question now is, did it fail to export or we never tried?
                    if export_record is not None:
                        logger.error("File %s not found on archive and had error in export queue" % basefilename)
                        print("File %s not found on archive and had error in export queue" % basefilename)
                    else:
                        logger.error("File %s not found on archive and not found in export queue" % basefilename)
                        print("File %s not found on archive and not found in export queue" % basefilename)

logger.info("*** verify_exported.py exiting normally at %s" % datetime.datetime.now())
