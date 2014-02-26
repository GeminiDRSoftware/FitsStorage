from boto.s3.connection import S3Connection
from boto.s3.key import Key

from fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
from logger import logger, setdebug, setdemon
import os

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", dest="file", help="Filename to upload")
parser.add_option("--path", action="store", dest="path", default="/net/wikiwiki/dataflow", help="Path to directory where file is")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()
path = options.path
file = options.file

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  s3-simple-upload starting")

fullpath = os.path.join(path, file)

s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)

k = Key(bucket)
k.key = file
k.set_contents_from_filename(fullpath)

logger.info("Uploaded size is %d" % k.size)
logger.info("Uploaded MD5 is %s" % k.md5)

logger.info("**** done")
