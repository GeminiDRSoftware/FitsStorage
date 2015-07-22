import datetime
import sys

from sqlalchemy import join, desc

from fits_storage.fits_storage_config import using_s3, aws_access_key, aws_secret_key, s3_bucket_name
from fits_storage.logger import logger, setdebug, setdemon
from boto.s3.connection import S3Connection


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# Annouce startup
logger.info("*********    s3-count_files - starting up at %s" % datetime.datetime.now())

if(using_s3 == False):
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

logger.info("Querying files from S3 bucket: %s" % s3_bucket_name)
s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)
filelist = []
for key in bucket.list():
    filelist.append(key.name)

logger.info("Counting...")

dict = {
        'N2000': 0,
        'S2000': 0,
        'N2001': 0,
        'S2001': 0,
        'N2002': 0,
        'S2002': 0,
        'N2003': 0,
        'S2003': 0,
        'N2004': 0,
        'S2004': 0,
        'N2005': 0,
        'S2005': 0,
        'N2006': 0,
        'S2006': 0,
        'N2007': 0,
        'S2007': 0,
        'N2008': 0,
        'S2008': 0,
        'N2009': 0,
        'S2009': 0,
        'N2010': 0,
        'S2010': 0,
        'N2011': 0,
        'S2011': 0,
        'N2012': 0,
        'S2012': 0,
        'N2013': 0,
        'S2013': 0,
        'N2014': 0,
        'S2014': 0}

for file in filelist:
    for key in dict.keys():
        if file[:5] == key:
            dict[key] += 1

logger.info("done")
keys = dict.keys()
keys.sort()
for key in keys:
    logger.info("%s: %d" % (key, dict[key]))

logger.info("** s3-count_files.py exiting normally")
