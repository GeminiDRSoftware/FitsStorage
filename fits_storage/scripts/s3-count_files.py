import datetime
import sys
from collections import defaultdict

from sqlalchemy import join, desc

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.aws_s3 import S3Helper

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# Annouce startup
logger.info("*********    s3-count_files - starting up at %s" % datetime.datetime.now())

if not using_s3:
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

logger.info("Querying files from S3 bucket: %s" % s3_bucket_name)

logger.info("Counting...")

dct = defaultdict(int)

s3 = S3Helper()
for key in s3.bucket.list():
    dct[key[:5]] += 1

logger.info("done")

for key, value in sorted(dct.items()):
    site, year = key[0], key[1:5]
    if site in 'NS' and year.isdigit() and int(year) > 1999:
        logger.info("{}: {}".format(key, value))

logger.info("** s3-count_files.py exiting normally")
