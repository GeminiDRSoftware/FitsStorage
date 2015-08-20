from boto.s3.connection import S3Connection
from boto.s3.key import Key

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.aws_s3 import S3Helper
from fits_storage.utils.hashes import md5sum

import os
from multiprocessing import Pool

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", dest="file", help="Filename to upload")
parser.add_option("--all", action="store_true", dest="all", help="Upload all files in directory")
parser.add_option("--sort", action="store_true", dest="sort", help="Sort file list before uploading")
parser.add_option("--path", action="store", dest="path", default="/net/wikiwiki/dataflow", help="Path to directory where file is")
parser.add_option("--file-sw", action="store", dest="filesw", help="Starts-with filename filter, for use with --all")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--threads", action="store", dest="threads", help="Run this many threads in parallel")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

options, args = parser.parse_args()
file = options.file

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)
logger.info("Connecting to S3 and getting bucket")
s3 = S3Helper()
path = options.path

def log(logger, text, *args):
    if logger:
        logger.info(text, *args)
    else:
        print text % args

def do_file(filename, logger=None):
    fullpath = os.path.join(path, filename)
    if not os.path.isfile(fullpath):
        log("%s is not a regular file - skipping", filename)
        return

    # See if it already exists in S3
    k = s3.bucket.get_key(filename)
    if k:
        # Yes, it exists, get anbd check the md5s
        s3md5 = s3.get_md5(k)
        filemd5 = md5sum(fullpath)
        if s3md5 == filemd5:
            # Identical file already there
            log("%s: Already exists with size %d and MD5 %s", filename, k.size, s3md5)
        else:
            # Exists but is wrong version
            log("%s: Already exists but is wrong MD5 - deleting", filename)
            s3.bucket.delete_key(filename)
            k = None

    if k is None:
        log("Uploading %s", filename)
        k = Key(s3.bucket)
        k.key = filename
        k.set_contents_from_filename(fullpath)
        log("%s: Uploaded size, MD5  is %d, %s", filename, k.size, k.md5)

# Announce startup
logger.info("*********  s3-simple-upload starting")

if options.all:
    file_list = []
    if options.filesw:
        logger.info("Filtering file list")
        for filename in os.listdir(path):
            if filename.startswith(options.filesw) and os.path.isfile(os.path.join(path, filename)):
                file_list.append(filename)
        logger.info("- found %d files" % len(file_list))
    else:
        # Get a listing of the directory
        file_list = os.listdir(path)
        logger.info("All files mode - found %d files" % len(file_list))
else:
    logger.info("Single file mode: %s" % file)
    file_list = [file]

if options.sort:
    # Sort the file list
    logger.info("Sorting the file list")
    file_list.sort()

if options.threads is None:
    for filename in file_list:
        do_file(filename, logger)
else:
    pool = Pool(int(options.threads))
    pool.map(do_file, file_list)

logger.info("**** done")

