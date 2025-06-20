#!/usr/bin/env python3

import datetime
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.config import get_config
fsc = get_config()

if fsc.using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper

"""
Utility for validating `~DiskFile`s as being present.

This script checks a set of files to check if they are present and marks 
them not present if they are not available.  This can be useful in bootstrapping
a system where you mark all canonical `~DiskFile`s present and run this to
clear out the ones that are no longer on disk.
"""

parser = OptionParser()
parser.add_option("--limit", action="store", type="int",
                  help="specify a limit on the number of files to examine. The "
                       "list is sorted by lastmod time before the limit is "
                       "applied")
parser.add_option("--file-pre", action="store", type="string", dest="filepre",
                  help="File prefix to check (omit for all)")
parser.add_option("--checkmd5", action="store_true", dest="checkmd5",
                  help="Check md5sums of files that exist against the database")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   rollcall.py - starting up at %s", datetime.datetime.now())

# Get a database session
with session_scope() as session:
    # Get a list of all diskfiles marked as present
    query = session.query(DiskFile).filter(DiskFile.present == True)\
        .order_by(DiskFile.lastmod)

    if options.filepre:
        query = query.filter(DiskFile.filename.startswith(options.filepre))

    # Did we get a limit option?
    if options.limit:
        query = query.limit(options.limit)

    logger.info("evaluating number of rows...")
    n = query.count()
    logger.info("%d files to check", n)

    logger.info("Starting checking...")

    if fsc.using_s3:
        logger.debug("Connecting to s3")
        s3 = Boto3Helper()

    i = 0
    missingfiles = []
    badfiles = []
    goodfiles = []
    for df in query:
        if fsc.using_s3:
            logger.debug("Getting s3 key for %s", df.filename)
            exists = s3.exists_key(df.filename)
        else:
            # Does the file actually exist?
            exists = df.file_exists()

        if exists is False:
            df.present = False
            logger.info("File %d/%d: Marking file %s (diskfile id %d) as not "
                        "present", i, n, df.filename, df.id)
            missingfiles.append(df.filename)
            session.commit()
        else:
            if (i % 1000) == 0:
                logger.info("File %d/%d: present", i, n)
            if options.checkmd5:
                if fsc.using_s3:
                    logger.error("md5 check with S3 not implemented")
                else:
                    file_md5 = df.get_file_md5()
                    logger.debug("File md5: %s  DB md5: %s",
                                 file_md5, df.file_md5)
                    if file_md5 != df.file_md5:
                        logger.warning("MD5 mismatch between file and DB: %s",
                                       df.filename)
                        badfiles.append(df.filename)
                    else:
                        goodfiles.append(df.filename)
        i += 1

    if len(badfiles):
        logger.info("\n %d files failed md5 check", len(badfiles))
    if len(goodfiles):
        logger.info("\n %d files passed md5 check", len(goodfiles))
    if len(missingfiles):
        logger.warning("\nMarked %d files as no longer present\n%s\n",
                       len(missingfiles), missingfiles)
    else:
        logger.info("All files present")

logger.info("*** rollcall.py exiting normally at %s" % datetime.datetime.now())
