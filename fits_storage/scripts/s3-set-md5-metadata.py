#!/usr/bin/env python

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdemon, setdebug

import sys
import datetime

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--filepre", action="store", dest="filepre", help="File prefix to operate on")
options, args = parser.parse_args()
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    s3-set-md5-metadata - starting up at %s" % datetime.datetime.now())

if not using_s3:
    logger.error("This script is only useable on installations using S3 for storage")
    sys.exit(-1)

from fits_storage.utils.aws_s3 import get_helper
from fits_storage.utils.hashes import md5sum, md5sum_size_fp

s3 = get_helper()
logger.info("Querying files from S3 bucket: %s" % s3.bucket.name)

def fetch_and_compute(keyname):
    with s3.fetch_temporary(keyname, skip_tests=True) as fileobj:
        return md5sum_size_fp(fileobj)[0]

for objsum in s3.bucket.objects.all():
    name = objsum.key
    if name.endswith('_preview.jpg'):
        continue
    if options.filepre and not name.startswith(options.filepre):
        continue

    if s3.get_md5(name):
        logger.info("Found metadata for {}".format(name))
        continue

    etag = s3.get_etag(s3.get_key(name)).replace('"', '')
    computed_md5 = None
    try:
        if ('-' not in etag) and (len(etag) == 32) and int(etag, 16):
            computed_md5 = etag
    except ValueError:
        pass
    s3.set_metadata(name, md5=(computed_md5 if computed_md5 is not None else fetch_and_compute(name)))
    logger.info("Metadata MD5 set for {}".format(name))
