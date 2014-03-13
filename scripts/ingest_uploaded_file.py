import os
import sys
import traceback
import datetime

from orm import sessionfactory
from fits_storage_config import storage_root, upload_staging_path, processed_cals_path, using_s3
from logger import logger, setdemon, setdebug
from utils.add_to_ingestqueue import addto_ingestqueue

if(using_s3):
    from fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--filename", action="store", type="string", dest="filename", help="filename of uploaded file to ingest")
parser.add_option("--processed_cal", action="store", type="string", dest="processed_cal", help="Boolean, says whether file is a processed_cal")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

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

# Move the file to it's appropriate loaction in storage_root/path or S3

# Construct the full path names and move the file into place
src = os.path.join(upload_staging_path, options.filename)
dst = os.path.join(path, options.filename)

if(using_s3):
    # Copy to S3
    try:
        logger.debug("Connecting to S3")
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        k = Key(bucket)
        k.key = dst
        logger.info("Uploading %s to S3 as %s" % (src, dst))
        k.set_contents_from_filename(src)
        os.unlink(src)
    except:
        string = traceback.format_tb(sys.exc_info()[2])
        string = "".join(string)
        logger.error("Exception during S3 upload: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

else:
    dst = os.path.join(storage_root, dst)
    logger.debug("Moving %s to %s" % (src, dst))
    # We can't use os.rename as that keeps the old permissions and ownership, which we specifically want to avoid
    fin = open(src, 'r')
    fout = open(dst, 'w')
    # this is a bit brute force
    buf = fin.read()
    fout.write(buf)
    buf = None
    fin.close()
    fout.close()
    os.unlink(src)

session = sessionfactory()

logger.info("Queueing for Ingest: %s" % dst)
addto_ingestqueue(session, options.filename, path)

session.close()
logger.info("*** ingest_uploaded_file.py exiting normally at %s" % datetime.datetime.now())

