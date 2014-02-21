import sys
sys.path.append("/opt/boto/lib/python2.6/site-packages/boto-2.23.0-py2.6.egg")

from boto.s3.connection import S3Connection

from FitsStorageConfig import s3_bucket_name, aws_access_key, aws_secret_key
from logger import logger, setdebug, setdemon
import os

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  s3-listing starting")

s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)

for key in bucket.list():
    logger.info("name: %s, content_type: %s, last_modified: %s, size: %s, md5: %s" % (key.name, key.content_type, key.last_modified, key.size, key.md5))

logger.info("**** done")
