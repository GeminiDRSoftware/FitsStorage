import datetime
import sys

from sqlalchemy import join, desc

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.aws_s3 import get_helper


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
#parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure", default=False, help="Needed for sanity check")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    s3_nuke.py - starting up at %s" % datetime.datetime.now())

if not using_s3:
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

if not options.yesimsure:
    logger.info("This is a really dangerous script to run. If you're not sure, don't do this.")
    logger.info("This will unconditionally delete files from the S3 storage")
    logger.error("You need to say --yesimsure to make it work")
    sys.exit(2)


# Get a full listing from S3. The preview files might not be in the DB.
s3 = get_helper()
logger.info("Getting file list from S3")
logger.info("Bucket is %s", str(s3.bucket))
for key in s3.list_keys():
    logger.info("Deleting %s" % s3.get_name(key))
    key.delete()

logger.info("** s3_nuke.py exiting normally")
