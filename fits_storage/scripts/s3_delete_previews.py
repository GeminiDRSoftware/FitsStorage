import datetime
import sys

from sqlalchemy import join, desc

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.aws_s3 import get_helper

from multiprocessing import Pool, Process, Queue

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

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

def feed_names():
    # Get a full listing from S3. The preview files might not be in the DB.
    logger.info("Getting file list from S3")

    for obj in s3.bucket.objects.all():
        name = obj.key
        if name.endswith("_preview.jpg"):
            logger.info("Found {}".format(name))
            yield name

# Annouce startup
logger.info("*********    s3_delete_previews.py - starting up at %s" % datetime.datetime.now())

if not using_s3:
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(1)

if not (options.yesimsure or options.count):
    logger.info("This is a really dangerous script to run. If you're not sure, don't do this.")
    logger.info("This will unconditionally delete files from the S3 storage")
    logger.error("You need to say --yesimsure to make it work")
    sys.exit(2)

s3 = get_helper()
#filelist = filter(lambda n: n.endswith("_preview.jpg"), s3.key_names())

if options.count:
    logger.info("Found %d preview files", len(filelist))
    sys.exit(0)

if options.dryrun:
    def delete_it(filename):
        return "Dryrun - not actually Deleting, {}".format(filename)
else:
    def delete_it(filename):
#        if logger:
#            logger.info("Deleting %s", filename)
#        else:
#            print "Deleting %s" % filename
        s3.get_key(filename).delete()
        return "Removed {}".format(filename)

if options.threads:
    threads = int(options.threads)
    logger.info("Starting parallel delete with %d threads", threads)
    pool = Pool(threads)
    for result in pool.imap_unordered(delete_it, feed_names(), chunksize=100):
        logger.info(result)
else:
    for filename in feed_names():
        logger.info(delete_it(filename))

logger.info("** s3_delete_previews.py exiting normally")
