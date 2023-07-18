#!/usr/bin/env python3

import sys
import os
import datetime
import tarfile
from optparse import OptionParser

from sqlalchemy import join
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from fits_storage.db import session_scope
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.tapeutils import TapeDrive
from fits_storage.db.list_headers import list_headers
from fits_storage.db.selection import getselection, openquery

from fits_storage.config import get_config


# Option Parsing
parser = OptionParser()
parser.add_option("--selection", action="store", type="string",
                  dest="selection",
                  help="the file selection criteria to use. This is a / "
                       "separated list like in the URLs. Can be a date or "
                       "daterange for example")
parser.add_option("--tapedrive", action="append", type="string",
                  dest="tapedrive",
                  help="tapedrive to use. Give this option multiple times to "
                       "specify multiple drives")
parser.add_option("--tapelabel", action="append", type="string",
                  dest="tapelabel",
                  help="tape label of tape. Give this option multiple times to "
                       "specify multiple tapes. Give the tapedrive and "
                       "tapelabel arguments in the same order.")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--dontcheck", action="store_true", dest="dontcheck",
                  help="Don't rewind and check the tape label in the drive, "
                       "go direct to eod and write")
parser.add_option("--skip", action="store_true", dest="skip",
                  help="Skip files that are already on any tape")
parser.add_option("--nodeduplicate", action="store_true", dest="nodedup",
                  help="Do Not skip files that are already successfully "
                       "written to this tape or any of these tapes")
parser.add_option("--auto", action="store_true", dest="auto",
                  help="Automatically construct selection for cron job")
parser.add_option("--ndays", action="store", type="int", dest="ndays",
                  default=14, help="Number of days for auto mode")
parser.add_option("--skipdays", action="store", type="int", dest="skipdays",
                  default=10, help="Number of days to skip for auto mode")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   write_to_tape.py - starting up at %s",
            datetime.datetime.now())
fsc = get_config()


if (not options.selection) and (not options.auto):
    logger.error("You must specify a file selection")
    sys.exit(1)

if len(options.tapedrive) < 1:
    logger.error("You must specify a tape drive")
    sys.exit(1)

if len(options.tapedrive) != len(options.tapelabel):
    logger.error("You must specify the same number of tape drives as "
                 "tape labels")
    sys.exit(1)

if options.auto:
    utcnow = datetime.datetime.utcnow()
    utcend = utcnow - datetime.timedelta(days=options.skipdays)
    utcstart = utcend - datetime.timedelta(days=options.ndays)
    daterange = "%s-%s" % (utcstart.date().strftime("%Y%m%d"),
                           utcend.date().strftime("%Y%m%d"))
    # If ndays == 1 then just do a single date
    if options.ndays == 1:
        daterange = "%s" % utcend.date().strftime("%Y%m%d")
    options.selection = daterange
    options.skip = True

options.selection += "/present"

logger.info("TapeDrive: %s; TapeLabel: %s",
            options.tapedrive, options.tapelabel)

# Generate a file list from the selection
logger.info("Building the file list")
things = options.selection.split('/')
selection = getselection(things)
logger.info("Selection: %s", selection)
logger.info("Selection is open: %s", openquery(selection))

with session_scope() as session:
    logger.info("Getting header object list")
    orderby = ['ut_datetime']
    headers = list_headers(selection, orderby, session=session, unlimit=True)

    # For some reason, looping through the header list directly for the add
    # is really slow if the list is big.
    logger.info("Building diskfile list")
    diskfiles = []
    for header in headers:
        diskfiles.append(header.diskfile)
    headers = None

    # Make a list containing the tape device objects
    tapedrives = [TapeDrive(tapedrive, fsc.fits_tape_scratchdir, logger=logger)
                  for tapedrive in options.tapedrive]

    # Get the database tape object for each tape label given
    logger.debug("Finding tape records in DB")
    tapes = []
    for tapelabel in options.tapelabel:
        query = session.query(Tape).filter(Tape.active == True)\
            .filter(Tape.label == tapelabel)

        try:
            tape = query.one()
        except NoResultFound:
            logger.error("Could not find active tape with label %s", tapelabel)
            session.close()
            sys.exit(1)
        except MultipleResultsFound:
            logger.error("Multiple active tapes with label %s:", tapelabel)
            session.close()
            sys.exit(1)
        tapes.append(tape)
        logger.debug("Found tape id in database: %d, label: %s",
                     tape.id, tape.label)
        if tape.full:
            logger.error("Tape labeled %s is full according to the DB. Exiting",
                         tape.label)
            sys.exit(2)

    tapeids = {t.id for t in tapes}

    if options.nodedup:
        logger.info("Nodeduplicate option given - not skipping files "
                    "already on any of these tapes")
    else:
        logger.info("Checking for duplication on these tapes")
        deduplicated_diskfiles = []
        for df in diskfiles:
            numtapes = session.query(Tape)\
                .select_from(join(TapeFile, join(TapeWrite, Tape)))\
                .filter(Tape.active == True)\
                .filter(TapeWrite.succeeded == True)\
                .filter(TapeFile.filename == df.filename)\
                .filter(TapeFile.md5 == df.file_md5)\
                .filter(Tape.id.in_(tapeids))\
                .count()

            if numtapes == 0:
                # this file is not on any of these tapes, include it
                logger.debug("File not on these tapes, not de-duping it")
                deduplicated_diskfiles.append(df)
            else:
                logger.debug("File %s is on at least one of these tapes, "
                             "removing it from list as a duplicate",
                             df.filename)

        diskfiles = deduplicated_diskfiles

    if options.skip:
        logger.info("Checking for duplication to any tapes")
        actual_diskfiles = []
        for df in diskfiles:
            num = session.query(TapeFile)\
                .select_from(join(TapeFile, join(TapeWrite, Tape)))\
                .filter(Tape.active == True)\
                .filter(TapeWrite.succeeded == True)\
                .filter(TapeFile.filename == df.filename)\
                .filter(TapeFile.md5 == df.file_md5)\
                .count()

            if num == 0:
                actual_diskfiles.append(df)
                logger.debug("Not skipping file %s as it is on 0 tapes",
                             df.filename)
            else:
                logger.debug("Skipping File %s : is already on tape %d times",
                             df.filename, num)

        diskfiles = actual_diskfiles

    # At this point, diskfiles is the list of diskfiles to go to tape.
    numfiles = len(diskfiles)
    totalsize = 0
    for df in diskfiles:
        totalsize += df.file_size
    logger.info("Got %d files totalling %.2f GB to write to tape",
                numfiles, (totalsize / 1.0E9))

    if numfiles == 0:
        logger.info("Exiting - no files")
        exit(0)

    # Check the tape label in the drives
    if not options.dontcheck:
        for tapelabel, td in zip(options.tapelabel, tapedrives):
            logger.info("Checking tape label in drive %s", td.dev)
            if td.online() is False:
                logger.error("No tape in drive %s", td.dev)
                session.close()
                sys.exit(1)
            thislabel = td.readlabel()
            if thislabel != tapelabel:
                logger.error("Label of tape in drive %s: %s does not match "
                             "label given as %s",
                             td.dev, thislabel, tapelabel)
                session.close()
                sys.exit(1)
            logger.info("OK - found tape in drive %s with label: %s",
                        td.dev, thislabel)

    # check md5sums match what's on disk.
    logger.info("Verifying md5s")
    for df in diskfiles:
        actual_md5 = df.get_file_md5()
        db_md5 = df.file_md5
        if actual_md5 != db_md5:
            logger.error("md5sum mismatch for file %s: file: %s, database: %s",
                         df.filename, actual_md5, db_md5)
            session.close()
            sys.exit(1)

    logger.info("All file md5s verified OK")

    # Now loop through the tapes, doing all the stuff on each
    oldcwd = os.getcwd()
    os.chdir(fsc.storage_root)
    for td, tape in zip(tapedrives, tapes):
        logger.debug("About to write on tape label %s in drive %s",
                     tape.label, td.dev)
        # Position Tape
        if not options.dryrun:
            logger.info("Positioning Tape %s", td.dev)
            td.setblk0()
            td.eod(fail=True)

            if td.eot():
                logger.error("Tape %s in %s is at End of Tape. Tape is Full. "
                             "Marking tape as full in DB and aborting",
                             tape.label, td.dev)
                tape.full = True
                session.commit()
                td.cleanup()
                session.close()
                os.chdir(oldcwd)
                sys.exit(1)

            # Update tape first/lastwrite
            logger.debug("Updating tape record for tape label %s", tape.label)
            if tape.firstwrite is None:
                tape.firstwrite = datetime.datetime.utcnow()
            tape.lastwrite = datetime.datetime.utcnow()
            session.commit()

            # Create tapewrite record
            logger.debug("Creating TapeWrite record for tape %s", tape.label)
            tw = TapeWrite()
            tw.tape_id = tape.id
            session.add(tw)
            session.commit()
            # Update tapewrite values pre-write
            tw.beforestatus = td.status()
            tw.filenum = td.fileno()
            tw.startdate = datetime.datetime.utcnow()
            tw.hostname = os.uname()[1]
            tw.tapedrive = td.dev
            tw.succeeded = False
            session.commit()

            # Write the tape.
            bytecount = 0
            blksize = 64 * 1024

            logger.info("Creating tar archive on tape %s on drive %s",
                        tape.label, td.dev)
            try:
                tar = tarfile.open(name=td.dev, mode='w|', bufsize=blksize)
                tarok = True
            except Exception:
                logger.error("Error opening tar archive", exc_info=True)
                tarok = False

            backup_files = list()
            for df in diskfiles:
                filename = df.filename

                try:
                    statinfo = os.stat(df.fullpath)
                    f = open(df.fullpath, "rb")
                    tarinfo = tarfile.TarInfo(df.fullpath)
                    tarinfo.size = statinfo.st_size  # tarinfo.size
                    tarinfo.mtime = statinfo.st_mtime
                    tarinfo.mode = statinfo.st_mode
                    tarinfo.type = tarfile.REGTYPE
                    tarinfo.uid = statinfo.st_uid
                    tarinfo.gid = statinfo.st_gid

                    logger.info("Adding %s to tar file on tape %s in drive %s",
                                df.filename, tape.label, td.dev)
                    tar.addfile(tarinfo, f)
                    f.close()
                    # Create the TapeFile entry and add to DB
                    tapefile = TapeFile()
                    tapefile.tapewrite_id = tw.id
                    tapefile.filename = df.filename
                    tapefile.md5 = df.file_md5
                    tapefile.size = df.file_size
                    tapefile.lastmod = df.lastmod
                    tapefile.compressed = df.compressed
                    tapefile.data_size = df.data_size
                    tapefile.data_md5 = df.data_md5
                    session.add(tapefile)
                    # Do the commit at the end for speed.
                    bytecount += df.file_size
                except Exception:
                    logger.error("Exception adding file to tar archive",
                                 exc_info=True)
                    logger.info("Probably the tape filled up - Marking tape as "
                                "full in the DB - label: %s", tape.label)
                    tape.full = True
                    session.commit()
                    tarok = False
                    break

            session.commit()
            logger.info("Completed writing tar archive on tape %s in drive %s",
                        tape.label, td.dev)
            logger.info("Wrote %.2f GB ", bytecount/1.0E9)
            try:
                tar.close()
            except Exception:
                logger.error("Exception closing tar archive", exc_info=True)
                tarok = False

            # update records post-write
            logger.debug("Updating tapewrite record")
            tw.enddate = datetime.datetime.utcnow()
            logger.debug("Succeeded: %s", tarok)
            tw.succeeded = tarok
            tw.afterstatus = td.status()
            tw.size = bytecount
            session.commit()

    os.chdir(oldcwd)
    session.close()
    logger.info("*** write_to_tape exiting normally at %s",
                datetime.datetime.now())
