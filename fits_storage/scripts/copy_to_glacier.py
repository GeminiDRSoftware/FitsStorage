#! /usr/bin/env python3

import datetime
import argparse

from sqlalchemy import and_

from fits_storage.db import sessionfactory
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.server.aws_s3 import Boto3Helper

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.server.orm.glacier import Glacier
from fits_storage.config import get_config


# This is the amount of rows to be retrieved at the same time from the database
# Controls the resources taken in the local side. Too small number will result
# in high database traffic. Too big number may cause a significant delay in the
# operations, and a large memory consumption on the client side.
FETCH_SIZE = 1000


parser = argparse.ArgumentParser(description='Copy data to Glacier')
parser.add_argument('--filepre', dest='filepre', action='store',
                    help="Select files with given filename prefix")
parser.add_argument('--daysold', dest='daysold', action='store', type=int,
                    default=14, help="Select files with a lastmod time more "
                                     "than N days ago. Note, defaults to 14."
                                     "To disable this, set to 0.")
parser.add_argument('--numdays', dest='numdays', action='store', type=int,
                    default=0, help="Select files with a lastmod time at most "
                                    "M days older than more than --daysold")
parser.add_argument('--limit', dest='limit', action='store', type=int,
                    default=None, help="Limit the number of files to transfer")
parser.add_argument("--dryrun", dest="dryrun", action="store_true",
                    default=None, help="Don't actually copy files")
parser.add_argument('--debug', dest='debug', action='store_true',
                    help="Increase log level to debug")
parser.add_argument('--demon', dest='demon', action='store_true',
                    help="Do not generate output to stdout")
args = parser.parse_args()

fsc = get_config()
setdebug(args.debug)
setdemon(args.demon)

logger.info("***   copy_to_glacier.py - starting up at %s",
            datetime.datetime.now())

s3 = Boto3Helper(logger=logger)

if fsc.s3_glacier_bucket_name:
    logger.info("Will copy to bucket: %s", fsc.s3_glacier_bucket_name)
else:
    logger.error("No s3_glacier_bucket_name config defined. Exiting")
    exit(1)

try:
    with PidFile(logger) as pidfile:
        session = sessionfactory()

        # There's an issue where it won't copy files > 5GB...

        # Find present diskfile versions that are simply not in glacier
        query = session.query(DiskFile).\
            outerjoin(Glacier, (and_(DiskFile.filename == Glacier.filename,
                                     DiskFile.file_md5 == Glacier.md5)))\
            .filter(DiskFile.present == True)\
            .filter(Glacier.id.is_(None))

        if args.filepre:
            query = query.filter(DiskFile.filename.startswith(args.filepre))

        if args.daysold:
            # Select files that are at least this many days old.
            # ie lastmod older than (less than) point in time.
            # [These Files] ---daysold--- [Now]
            interval = datetime.timedelta(days=args.daysold)
            maxdate = datetime.datetime.now() - interval
            query = query.filter(DiskFile.lastmod < maxdate)

        if args.numdays:
            # Select files with lastmod between daysold days old and
            # numdays+daysold days old.
            # ie lastmod older than (less than) daysold, and newer than
            # (greater than) daysold+numdays
            # daysold can be in fact zero, which selects the last numdays.
            # ... [daysold+numdays ago] [These Files] ---daysold--- [Now]
            daysold = args.daysold if args.daysold else 0
            maxdate = datetime.timedelta(days=daysold)
            mindate = datetime.timedelta(days=daysold+args.numdays)
            query = query.filter(DiskFile.lastmod < maxdate)
            query = query.filter(DiskFile.lastmod > mindate)

        if args.limit:
            query = query.limit(args.limit)

        logger.info("Found %d files to copy", query.count())

        for diskfile in query.yield_per(FETCH_SIZE):
            if args.dryrun:
                logger.info("Dryrun - not actually copying %s to Glacier",
                            diskfile.filename)
                continue
            logger.info("Copying %s to Glacier", diskfile.filename)
            s3.copy(diskfile.filename, to_bucket=fsc.s3_glacier_bucket_name)
            glacier = Glacier()
            glacier.filename = diskfile.filename
            glacier.md5 = diskfile.file_md5
            glacier.when_uploaded = datetime.datetime.utcnow()
            session.add(glacier)
        session.commit()
except PidFileError as e:
    logger.error(str(e))

logger.info("***   copy_to_glacier.py - exiting at %s", datetime.datetime.now())
