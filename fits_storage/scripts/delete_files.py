#!/usr/bin/env python3

import sys
import datetime
import os
import smtplib
from sqlalchemy import desc
from optparse import OptionParser

from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.server.tapeutils import FileOnTapeHelper

from fits_storage.config import get_config
fsc = get_config()

parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string",
                  dest="tapeserver", default=fsc.tape_server,
                  help="FitsStorage Tape server to check the files are on tape")
parser.add_option("--path", action="store", type="string", dest="path",
                  default="", help="Path within the storage root")
parser.add_option("--pathcontains", action="store", type="string",
                  dest="pathcontains", default="",
                  help="Path within the storage root contains string")
parser.add_option("--file-pre", action="store", type="string", dest="filepre",
                  help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--maxnum", type="int", action="store", dest="maxnum",
                  help="Delete at most X files.")
parser.add_option("--maxgbs", type="float", action="store", dest="maxgbs",
                  help="Delete at most X GB of files")
parser.add_option("--auto", action="store_true", dest="auto",
                  help="Delete old files to get to pre-defined free space")
parser.add_option("--olderthan", type="int", action="store", dest="olderthan",
                  help="Only delete files listed in database as older than this"
                       " number of days")
parser.add_option("--oldbyfilename", action="store_true", dest="oldbyfilename",
                  help="Sort by filename to determine oldest files. Default"
                       "is to sort by lastmod time from the database")
parser.add_option("--numbystat", action="store_true", dest="numbystat",
                  default=False, help="Use statvfs rather than database to "
                                      "determine number of files on the disk")
parser.add_option("--yesimsure", action="store_true", dest="yesimsure",
                  help="Needed when file count is large")
parser.add_option("--notpresent", action="store_true", dest="notpresent",
                  help="Include files that are marked as not present")
parser.add_option("--minage", type="int", action="store", dest="minage",
                  help="Minimum days old a file must be to be deleted")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes",
                  default=2, help="Minimum number of tapes file must be on to "
                                  "be eligible for deletion")
parser.add_option("--skip-md5", action="store_true", dest="skipmd5",
                  help="Do not bother to verify the md5 of the file on disk")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--emailto", action="store", type="string", dest="emailto",
                  help="Email address to send message to")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   delete_files.py - starting up at %s",
            datetime.datetime.now())

with session_scope() as session:
    query = session.query(DiskFile).filter(DiskFile.canonical == True)

    if options.auto:
        # chdir to the storage root to kick the automounter
        cwd = os.getcwd()
        os.chdir(fsc.storage_root)
        s = os.statvfs(fsc.storage_root)
        os.chdir(cwd)
        gbavail = s.f_bsize * s.f_bavail / (1024 * 1024 * 1024)
        if options.numbystat:
            numfiles = s.f_files - s.f_favail
        else:
            numfiles = session.query(DiskFile)\
                .filter(DiskFile.present == True).count()
        logger.debug("Disk has %d files present and %.2f GB available",
                     numfiles, gbavail)
        numtodelete = numfiles - fsc.target_max_files
        if numtodelete > 0:
            logger.info("Need to delete at least %d files", numtodelete)

        gbtodelete = fsc.target_gb_free - gbavail
        if gbtodelete > 0:
            logger.info("Need to delete at least %.2f GB", gbtodelete)

        if numtodelete <= 0 and gbtodelete <= 0:
            logger.info("In Auto mode and nothing needs deleting. Exiting")
            sys.exit(0)

    if options.filepre:
        query = query.filter(DiskFile.filename.startswith(options.filepre))

    if not options.notpresent:
        query = query.filter(DiskFile.present == True)

    if options.path:
        query = query.filter(DiskFile.path == options.path)

    if options.pathcontains:
        query = query.filter(DiskFile.path.contains(options.pathcontains))

    oldby = DiskFile.lastmod
    if options.oldbyfilename:
        oldby = DiskFile.filename

    if options.olderthan and options.olderthan > 0:
        dt = datetime.datetime.now() - \
             datetime.timedelta(days=options.olderthan)
        query = query.filter(oldby < dt)

    query = query.order_by(desc(oldby))

    if options.maxnum:
        query = query.limit(options.maxnum)
    cnt = query.count()

    if cnt == 0:
        logger.info("No Files found matching file-pre. Exiting")
        sys.exit(0)

    logger.info("Got %d files to consider for deletion", cnt)
    if cnt > 2000 and not options.yesimsure:
        logger.error("To proceed with this many files, "
                     "you must say --yesimsure")
        sys.exit(1)

    # We use the FileOnTapeHelper class here which provides caching..
    foth = FileOnTapeHelper(tapeserver=options.tapeserver, logger=logger)

    if options.filepre:
        logger.info("Pre-populating tape server results cache from filepre")
        foth.populate_cache(options.filepre)

    sumbytes = 0
    numfiles = 0
    firstfile = None
    lastfile = None

    for diskfile in query:
        logger.debug("Full path filename: %s", diskfile.fullpath)
        if not diskfile.file_exists():
            logger.error("Cannot access file %s", diskfile.fullpath)
            continue

        # Check if it is on the exportqueue
        neq = session.query(ExportQueueEntry)\
            .filter(ExportQueueEntry.filename == diskfile.filename)\
            .count()
        if neq:
            logger.info("File %s is on the Export Queue - skipping",
                        diskfile.filename)
            continue

        if options.skipmd5:
            logger.debug("Skipping md5 check")
            filemd5 = None
        else:
            filemd5 = diskfile.get_file_md5()
            if filemd5 != diskfile.file_md5:
                logger.error("File: %s has an md5sum mismatch between the "
                             "database and the actual file. Skipping",
                             diskfile.filename)
                continue
            else:
                logger.debug("Actual File MD5 and canonical database diskfile "
                             "MD5 match: %s", filemd5)

        # If we got here, either the md5 matches or was skipped.
        # If it was skipped, the filemd5 value is None
        # - we are using options.skipmd5 to skip *both* the check that the
        # file on disk actually matches the local database, and that that's
        # the same as the file on tape.

        # Check if it's on tape.
        data_md5 = None if options.skipmd5 else diskfile.data_md5
        tape_ids = foth.check_file(diskfile.filename, data_md5)
        if len(tape_ids) < options.mintapes:
            logger.info("File %s is only on %d tapes (%s), not deleting",
                        diskfile.filename, len(tape_ids), str(tape_ids))
            continue

        firstfile = diskfile.filename if firstfile is None else firstfile
        lastfile = diskfile.filename
        if options.dryrun:
            logger.info("Dryrun: not actually deleting file %s",
                        diskfile.fullpath)
        else:
            try:
                logger.info("Deleting file %s", diskfile.fullpath)
                os.unlink(diskfile.fullpath)
                logger.debug("Marking diskfile id %d as not present",
                             diskfile.id)
                diskfile.present = False
                session.commit()
            except Exception:
                logger.error("Could not delete %s", diskfile.fullpath,
                             exc_info=True)

        sumbytes += diskfile.file_size
        numfiles += 1
        if options.maxnum and numfiles >= options.maxnum:
            logger.info("Have deleted %d files (%.2f GB). Stopping per maxnum",
                        numfiles, sumbytes/1E9)
            break
        if options.maxgbs and sumbytes/1E9 >= options.maxgbs:
            logger.info("Have deleted %.2f GBs (%d files) Stopping per maxgbs",
                        sumbytes/1E9, numfiles)
            break

        if options.auto and numfiles >= numtodelete:
            logger.info("In auto mode and have deleted required number of"
                        "files, stopping")
            break
        if options.auto and sumbytes >= gbtodelete*1E9:
            logger.info("In auto mode and have deleted required number of"
                        "GBs, stopping")
            break

    logger.info("Deleted %d files totalling %.2f GB", numfiles, sumbytes/1E9)
    logger.info("First file was: %s", firstfile)
    logger.info("Last file was: %s", lastfile)
    
    if options.emailto:
        if options.dryrun:
            subject = "Dry run file delete report"
        else:
            subject = "File delete report"

        mailfrom = 'fitsdata@gemini.edu'
        mailto = [options.emailto]

        msglines = [
            "Deleted %d files totalling %.2f GB" % (numfiles, sumbytes/1E9),
            "First file was: %s" % firstfile,
            "Last file was: %s" % lastfile,
        ]

        message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % \
                  (mailfrom, ", ".join(mailto), subject, '\n'.join(msglines))

        server = smtplib.SMTP(fsc.smtp_server)
        server.sendmail(mailfrom, mailto, message)
        server.quit()

    logger.info("***   delete_files.py exiting normally at %s",
                datetime.datetime.now())
