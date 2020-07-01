#!/usr/bin/env python

from fits_storage.fits_storage_config import using_s3
from fits_storage.logger import logger, setdemon, setdebug

import sys
import datetime
from optparse import OptionParser


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
    parser.add_option("--filename", action="store", dest="filename", default="", help="Filename to query")
    options, args = parser.parse_args()
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup


    logger.info("*********    s3-get-info - starting up at %s" % datetime.datetime.now())

    if not using_s3:
        logger.error("This script is only useable on installations using S3 for storage")
        sys.exit(-1)

    from fits_storage.utils.aws_s3 import get_helper
    from fits_storage.utils.hashes import md5sum, md5sum_size_fp

    s3 = get_helper()
    logger.info("Querying files from S3 bucket: %s" % s3.bucket.name)

    name = options.filename
    with s3.fetch_temporary(name, skip_tests=True) as fileobj:
        md5, size = md5sum_size_fp(fileobj)

    logger.info("%s fetched and computed size: %d", name, size)
    logger.info("%s fetched and computed md5: %s", name, md5)
    logger.info("%s etag: %s", name, s3.get_etag(s3.get_key(name)).replace('"', ''))
    try:
        logger.info("%s metadata md5: %s", name, s3.get_md5(name))
    except KeyError:
        logger.info("%s has no metadata md5", name)

    logger.info("*********    s3-get-info - ending at %s" % datetime.datetime.now())

