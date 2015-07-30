import os
import sys
import traceback
import datetime
import shutil

from fits_storage.orm import sessionfactory
from fits_storage.orm.fileuploadlog import FileUploadLog

from fits_storage.fits_storage_config import storage_root, upload_staging_path, processed_cals_path, using_s3
from fits_storage.logger import logger, setdemon, setdebug
from fits_storage.utils.ingestqueue import add_to_ingestqueue

if(using_s3):
    from fits_storage.fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    from fits_storage.utils.aws_s3 import upload_file


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--filename", action="store", type="string", dest="filename", help="filename of uploaded file to ingest")
parser.add_option("--processed_cal", action="store", type="string", dest="processed_cal", help="Boolean, says whether file is a processed_cal")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
parser.add_option("--fileuploadlog_id", action="store", type="int", dest="fileuploadlog_id", help="ID of FileUploadLog database entry to update")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********  ingest_uploaded_file.py - starting up at %s" % now)

# Need a filename
if (not options.filename):
    logger.error("No filename specified, exiting")
    sys.exit(1)

if(options.processed_cal == "True"):
    path = processed_cals_path
else:
    path = ''

session = sessionfactory()
try:
    # Find the upload log entry to update
    if options.fileuploadlog_id:
        fileuploadlog = session.query(FileUploadLog).filter(FileUploadLog.id == options.fileuploadlog_id).one()
    else:
        fileuploadlog = None

    # Move the file to its appropriate loaction in storage_root/path or S3
    # Construct the full path names and move the file into place
    src = os.path.join(upload_staging_path, options.filename)
    dst = os.path.join(path, options.filename)

    if fileuploadlog:
        fileuploadlog.destination = dst

    if(using_s3):
        # Copy to S3
        try:
            logger.debug("Connecting to S3")
            s3conn = S3Connection(aws_access_key, aws_secret_key)
            bucket = s3conn.get_bucket(s3_bucket_name)
            logger.info("Uploading %s to S3 as %s" % (src, dst))
            if fileuploadlog:
                fileuploadlog.s3_ut_start = datetime.datetime.utcnow()
            ok = upload_file(bucket, dst, src, logger)
            if fileuploadlog:
                fileuploadlog.s3_ut_end = datetime.datetime.utcnow()
                fileuploadlog.s3_ok = ok
            os.unlink(src)
        except:
            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            fileuploadlog.add_note("Exception during S3 upload, see log file")
            logger.error("Exception during S3 upload: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

    else:
        dst = os.path.join(storage_root, dst)
        logger.debug("Moving %s to %s" % (src, dst))
        # We can't use os.rename as that keeps the old permissions and ownership, which we specifically want to avoid
        # Instead, we copy the file and the remove it
        shutil.copy(src, dst)
        os.unlink(src)
        if fileuploadlog:
            fileuploadlog.file_ok = True


    logger.info("Queueing for Ingest: %s" % dst)
    iq_id = add_to_ingestqueue(session, logger, options.filename, path)

    if fileuploadlog:
        fileuploadlog.ingestqueue_id = iq_id

except:
    string = traceback.format_tb(sys.exc_info()[2])
    string = "".join(string)
    logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
    raise

finally:
    session.commit()
    session.close()

logger.info("*** ingest_uploaded_file.py exiting normally at %s" % datetime.datetime.now())

