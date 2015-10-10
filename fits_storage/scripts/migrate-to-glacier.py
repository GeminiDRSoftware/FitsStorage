#! /usr/bin/env python
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.fits_storage_config import using_s3, s3_backup_bucket_name
from fits_storage.utils.pidfile import PidFile, PidFileError

from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.glacier import Glacier
from sqlalchemy import and_, or_, func, cast, between
from sqlalchemy import Interval
from sqlalchemy.exc import OperationalError

from fits_storage.utils.aws_s3 import get_helper

import sys
import os
import datetime
import time
import traceback

import argparse

parser = argparse.ArgumentParser(description='Migrate data in S3 to Glacier')
parser.add_argument('--file-pre', dest='filepre', action='store', metavar='PREF', help="File prefix to check (omit for all)")
parser.add_argument('--daysold', dest='daysold', action='store', metavar='N', type=int, default=14, help="Operate on diskfiles with a lastmode time more than N days ago")
parser.add_argument('--numdays', dest='numdays', action='store', metavar='M', type=int, default=0, help="Operate on diskfiles with a lastmode time at most M days older than more than --daysold")
parser.add_argument('--limit', dest='limit', action='store', metavar='L', type=int, default=None, help="Limit the number of files to transfer this run to L")
parser.add_argument('--debug', dest='debug', action='store_true', help="Increase log level to debug")
parser.add_argument('--quiet', dest='quiet', action='store_true', help="Do not generate output to stdout")

options = parser.parse_args()

setdebug(options.debug)
setdemon(options.quiet)

s3 = get_helper()

scriptname = os.path.basename(sys.argv[0])

logger.info("*********    %s - starting up at %s", scriptname, datetime.datetime.now())

try:
    with PidFile(logger, scriptname) as pidfile, session_scope() as session:
        query = (
            session.query(DiskFile).outerjoin(Glacier, (and_(DiskFile.filename == Glacier.filename,
                                                             DiskFile.file_md5 == Glacier.md5)))
                                   .filter(DiskFile.present == True)
                                   .filter(or_(Glacier.id.is_(None), DiskFile.lastmod > Glacier.when_uploaded))
        )
        if options.filepre:
            query = query.filter(DiskFile.filename.startswith(options.filepre))

        until = func.now() - cast('{} days'.format(options.daysold), Interval)
        if options.numdays:
            since = func.now() - cast('{} days'.format(options.daysold + options.numdays), Interval)
            query = query.filter(between(DiskFile.lastmod, since, until))
        else:
            query = query.filter(DiskFile.lastmod < until)
        if options.limit:
            query = query.limit(options.limit)

        for diskfile in query:
            logger.info("Copying {} to {}".format(diskfile.filename, s3_backup_bucket_name))
            s3.copy(diskfile.filename, to_bucket=s3_backup_bucket_name)
            glacier = Glacier()
            glacier.filename = diskfile.filename
            glacier.md5 = diskfile.file_md5
            now = datetime.datetime.now()
            glacier.when_uploaded = now
            #glacier.last_inventory = now
            session.add(glacier)
except PidFileError as e:
    logger.error(str(e))

logger.info("*********    %s - exiting at %s", scriptname, datetime.datetime.now())
