import datetime
import sys

from sqlalchemy import join, desc

from fits_storage.fits_storage_config import using_s3, aws_access_key, aws_secret_key, s3_bucket_name
from fits_storage.logger import logger, setdebug, setdemon
from boto.s3.connection import S3Connection

from multiprocessing import Pool

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
#parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure", default=False, help="Needed for sanity check")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
parser.add_option("--count", action="store_true", dest="count", help="Only count the number of previews, do not delete them")
parser.add_option("--threads", action="store", dest="threads", help="Run in parallel with this many threads")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# Annouce startup
logger.info("*********    s3_delete_previews.py - starting up at %s" % datetime.datetime.now())

if(using_s3 == False):
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

if options.yesimsure != True and options.count != True:
    logger.info("This is a really dangerous script to run. If you're not sure, don't do this.")
    logger.info("This will unconditionally delete files from the S3 storage")
    logger.error("You need to say --yesimsure to make it work")
    sys.exit(2)


# Get a full listing from S3. The preview files might not be in the DB.
logger.info("Getting file list from S3")
filelist = []
s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)
filelist = []
for key in bucket.list():
    if key.name.endswith("_preview.jpg"):
        filelist.append(key.name)

if options.count:
    logger.info("Found %d preview files", len(filelist))
    sys.exit(0)

def delete_it(filename, logger=None):
    key = bucket.get_key(filename)
    if logger:
        logger.info("Deleting %s", filename)
    else:
        print "Deleting %s" % filename
    key.delete()


if options.threads:
    threads = int(options.threads)
    logger.info("Starting parallel delete with %d threads", threads)
    pool = Pool(threads)
    pool.map(delete_it, filelist)
else:
    for filename in filelist:
        if filename.endswith("_preview.jpg"):
            if options.dryrun:
                logger.info("Dryrun - not actually Deleting %s", filename)
            else:
                delete_it(filename, logger=logger)
    
logger.info("** s3_delete_previews.py exiting normally")
