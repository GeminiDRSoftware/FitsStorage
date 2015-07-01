from boto.s3.connection import S3Connection
from boto.s3.key import Key

from fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
from logger import logger, setdebug, setdemon
import os

from utils.aws_s3 import get_s3_md5
from utils.hashes import md5sum

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", dest="file", help="Filename to upload")
parser.add_option("--all", action="store_true", dest="all", help="Upload all files in directory")
parser.add_option("--sort", action="store_true", dest="sort", help="Sort file list before uploading")
parser.add_option("--path", action="store", dest="path", default="/net/wikiwiki/dataflow", help="Path to directory where file is")
parser.add_option("--file-sw", action="store", dest="filesw", help="Starts-with filename filter, for use with --all")
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

file_list = []
if(options.all):
    if options.filesw:
        logger.info("Filtering file list")
        file_list = []
        for file in os.listdir(path):
            if file.startswith(options.filesw):
                file_list.append(file)
        logger.info("- found %d files" % len(file_list))
    else:
        # Get a listing of the directory
        file_list = os.listdir(path)
        logger.info("All files mode - found %d files" % len(file_list))
else:
    logger.info("Single file mode: %s" % file)
    file_list.append(file)

if(options.sort):
    # Sort the file list
    logger.info("Sorting the file list")
    file_list.sort()

logger.info("Connecting to S3 and getting bucket")
s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(s3_bucket_name)

for file in file_list:
    fullpath = os.path.join(path, file)

    # See if it already exists in S3
    k = bucket.get_key(file)
    if k:
        # Yes, it exists, get anbd check the md5s
        s3md5 = get_s3_md5(k)
        filemd5 = md5sum(fullpath)
        if s3md5 == filemd5:
            # Identical file already there
            logger.info("%s: Already exists with size %d and MD5 %s" % (file, k.size, s3md5))
        else:
            # Exists but is wrong version
            logger.info("%s: Already exists but is wrong MD5 - deleting" % file)
            bucket.delete_key(file)
            k = None

    if k is None:
        logger.info("Uploading %s" % file)
        k = Key(bucket)
        k.key = file
        k.set_contents_from_filename(fullpath)
        logger.info("%s: Uploaded size, MD5  is %d, %s" % (file, k.size, k.md5))

logger.info("**** done")
