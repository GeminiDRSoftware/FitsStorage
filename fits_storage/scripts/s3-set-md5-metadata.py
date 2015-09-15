#!/usr/bin/env python

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdemon, setdebug

import sys
import datetime

from multiprocessing import Pool, Process, Queue

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--threads", action="store", dest="threads", help="Number of threads to run")
options, args = parser.parse_args()
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

def feed_names():
    # Get a full listing from S3.
    logger.info("Getting file list from S3")

    for obj in s3.bucket.objects.all():
        yield obj.key

# Annouce startup
logger.info("*********    s3-set-md5-metadata - starting up at %s" % datetime.datetime.now())

if not using_s3:
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(-1)

from fits_storage.utils.aws_s3 import get_helper
from fits_storage.utils.hashes import md5sum, md5sum_size_fp

s3 = get_helper()

def fetch_and_compute(keyname):
    with s3.fetch_temporary(keyname, skip_tests=True) as fileobj:
        return md5sum_size_fp(fileobj)[0]

def process_it(name):
    try:
        if name.endswith('_preview.jpg'):
            return "skipping preview {}".format(name)

        if s3.get_md5(name):
            return "Found metadata for {}".format(name)

        etag = s3.get_etag(s3.get_key(name)).replace('"', '')
        computed_md5 = None
        try:
            if ('-' not in etag) and (len(etag) == 32) and int(etag, 16):
                computed_md5 = etag
        except ValueError:
            pass
        s3.set_metadata(name, md5=(computed_md5 if computed_md5 is not None else fetch_and_compute(name)))
        return "Metadata MD5 set for {}".format(name)
    except:
        return "ERROR processing {}".format(name)

if options.threads:
    threads = int(options.threads)
    logger.info("Starting parallel process with %d threads", threads)
    pool = Pool(threads)
    for result in pool.imap_unordered(process_it, feed_names(), chunksize=100):
        logger.info(result)
else:
    for filename in feed_names():
        logger.info(process_it(filename))


logger.info("*********    s3-set-md5-metadata - ending at %s" % datetime.datetime.now())

