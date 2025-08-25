#!/usr/bin/env python3

import sys
import datetime
import os
from optparse import OptionParser
from sqlalchemy import join

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile

parser = OptionParser()
parser.add_option("--filepre", action="store", type="string", dest="filepre",
                  help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--filere", action="store", type="string", dest="filere",
                  help="File regex to operate on, ie filename contains")
parser.add_option("--tapeset", action="append", type="int", dest="tapeset",
                  help="Tape set number to check file is on. "
                       "Can be given multiple times")
parser.add_option("--maxnum", type="int", action="store", dest="maxnum",
                  help="Delete at most N files.")
parser.add_option("--skip-md5", action="store_true", dest="skipmd5",
                  help="Do not verify the md5 of the file on disk")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   tapeserver_delete_files.py - starting up at %s",
            datetime.datetime.now())

if options.tapeset is None:
    logger.error("Must supply a tapeset to use this")
    exit(1)

with session_scope() as session:
    query = session.query(DiskFile).filter(DiskFile.present==True)\
        .order_by(DiskFile.filename)
    if options.filere:
        query = query.filter(DiskFile.filename.contains(options.filere))
    if options.filepre:
        query = query.filter(DiskFile.filename.startswith(options.filepre))
    if options.maxnum:
        query = query.limit(options.maxnum)
    cnt = query.count()

    if cnt == 0:
        logger.info("No Files found matching file-pre. Exiting")
        sys.exit(0)

    logger.info("Got %d files to consider for deletion", cnt)

    numdel = 0
    numskip = 0
    for diskfile in query:
        logger.debug("Full path filename: %s", diskfile.fullpath)
        if not diskfile.file_exists():
            logger.error("Cannot access file %s", diskfile.fullpath)
            continue
        if not options.skipmd5:
            filemd5 = diskfile.get_file_md5()
            dbmd5 = diskfile.file_md5
            logger.debug("Actual File MD5 and database MD5 are: %s and %s",
                         filemd5, dbmd5)
            if filemd5 != dbmd5:
                logger.error("File: %s has an md5sum mismatch between the "
                             "database and the actual file. Skipping",
                             diskfile.filename)
                continue
            else:
                logger.debug("File %s md5sum matches between database and file"
                             " on disk", diskfile.filename)
        else:
            logger.debug("Skipping md5sum check for file %s", diskfile.filename)

        # File exists on disk and md5 matches or check has been skipped.
        # Check if the file is actually on tape in all the tapesets given
        onall=True
        for ts in options.tapeset:
            tfs = session.query(TapeFile)\
                .select_from(join(join(TapeFile, TapeWrite), Tape))\
                .filter(TapeFile.filename == diskfile.filename)\
                .filter(TapeFile.md5 == diskfile.file_md5) \
                .filter(TapeWrite.succeeded == True)\
                .filter(Tape.active == True)\
                .filter(Tape.set == ts)\
                .count()

            logger.debug("Found %d tapefiles for %s on tapeset %d",
                         tfs, diskfile.filename, ts)

            if tfs == 0:
                # It's not on this tapeset
                onall = False
                logger.info("%s not found on tapeset %d - skipping",
                            diskfile.filename, ts)
                break

        if onall:
            # It's on all the tapesets...
            if options.dryrun:
                logger.info("Dry run - not actually deleting %s",
                            diskfile.filename)
            else:
                # Actually delete it
                try:
                    logger.info("Deleting %s", diskfile.filename)
                    os.unlink(diskfile.fullpath)
                    logger.debug("Marking diskfile id %d as not present",
                                 diskfile.id)
                    diskfile.present = False
                    session.commit()
                except Exception:
                    logger.error("Error deleting %s", diskfile.fullpath,
                                 exc_info=True)

logger.info("***   tapeserver_delete_files.py exiting normally at %s",
            datetime.datetime.now())
