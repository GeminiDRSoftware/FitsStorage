import sys
import os
import datetime
import time
import subprocess
import tarfile
import urllib.request, urllib.parse, urllib.error
import traceback
from bz2 import BZ2File
from tempfile import mkstemp

from sqlalchemy import join

from fits_storage.fits_storage_config import storage_root
from fits_storage.orm import session_scope
from fits_storage.orm.tapestuff import Tape, TapeWrite, TapeFile
from fits_storage.fits_storage_config import fits_tape_scratchdir
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.hashes import md5sum
from fits_storage.utils.tape import TapeDrive, get_tape_drive
from fits_storage.web.list_headers import list_headers
from fits_storage.web.selection import getselection, openquery



# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--selection", action="store", type="string", dest="selection", help="the file selection criteria to use. This is a / separated list like in the URLs. Can be a date or daterange for example")
parser.add_option("--tapedrive", action="append", type="string", dest="tapedrive", help="tapedrive to use. Give this option multiple times to specify multiple drives")
parser.add_option("--tapelabel", action="append", type="string", dest="tapelabel", help="tape label of tape. Give this option multiple times to specify multiple tapes. Give the tapedrive and tapelabel arguments in the same order.")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--dontcheck", action="store_true", dest="dontcheck", help="Don't rewind and check the tape label in the drive, go direct to eod and write")
parser.add_option("--skip", action="store_true", dest="skip", help="Skip files that are already on any tape")
parser.add_option("--nodeduplicate", action="store_true", dest="nodedup", help="Do Not skip files that are already sucessfully written to this tape or any of these tapes")
parser.add_option("--auto", action="store_true", dest="auto", help="Automatically construct selection for cron job")
parser.add_option("--ndays", action="store", type="int", dest="ndays", default=14, help="Number of days for auto mode")
parser.add_option("--skipdays", action="store", type="int", dest="skipdays", default=10, help="Number of days to skip for auto mode")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
parser.add_option("--compress", action="store_true", dest="compress",
                  help="Compress files with bzip2 before sending them to tape")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


class BackupFile:
    def __init__(self, bz2_filename, stagefilename, disk_file):
        self.bz2_filename = bz2_filename
        self.stagefilename = stagefilename
        self.disk_file = disk_file


# Annouce startup
logger.info("*********    write_to_tape.py - starting up at %s" % datetime.datetime.now())

if (not options.selection) and (not options.auto):
    logger.error("You must specify a file selection")
    sys.exit(1)

if len(options.tapedrive) < 1:
    logger.error("You must specify a tape drive")
    sys.exit(1)

if len(options.tapedrive) != len(options.tapelabel):
    logger.error("You must specify the same number of tape drives as tape labels")
    sys.exit(1)

if options.auto:
    utcnow = datetime.datetime.utcnow()
    utcend = utcnow - datetime.timedelta(days=options.skipdays)
    utcstart = utcend - datetime.timedelta(days=options.ndays)
    daterange = "%s-%s" % (utcstart.date().strftime("%Y%m%d"), utcend.date().strftime("%Y%m%d"))
    # If ndays == 1 then just do a single date
    if(options.ndays == 1):
        daterange = "%s" % utcend.date().strftime("%Y%m%d")
    options.selection = daterange
    options.skip = True
    autolog = logger.debug
else:
    autolog = logger.info

options.selection += "/present"

logger.info("TapeDrive: %s; TapeLabel: %s" % (options.tapedrive, options.tapelabel))

# Generate a file list from the selection
logger.info("Building the file list")
things = options.selection.split('/')
selection = getselection(things)
logger.info("Selection: %s" % selection)
logger.info("Selection is open: %s" % openquery(selection))

with session_scope() as session:
    logger.info("Getting header object list")
    orderby = []
    headers = list_headers(selection, orderby, session=session)

    # For some reason, looping through the header list directly for the add
    # is really slow if the list is big.
    logger.info("Building diskfile list")
    diskfiles = []
    for header in headers:
        diskfiles.append(header.diskfile)

    headers = None

    # Make a list containing the tape device objects
    tds = [get_tape_drive(tapedrive, fits_tape_scratchdir)
        for tapedrive in options.tapedrive]

    # Get the database tape object for each tape label given
    logger.debug("Finding tape records in DB")
    tapes = []
    for tapelabel in options.tapelabel:
        query = session.query(Tape).filter(Tape.label == tapelabel).filter(Tape.active == True)
        if(query.count() == 0):
            logger.error("Could not find active tape with label %s" % tapelabel)
            session.close()
            sys.exit(1)
        if(query.count() > 1):
            logger.error("Multiple active tapes with label %s:" % tapelabel)
            session.close()
            sys.exit(1)
        tape = query.one()
        tapes.append(tape)
        logger.debug("Found tape id in database: %d, label: %s" % (tape.id, tape.label))
        if(tape.full):
            logger.error("Tape with label %s is full according to the DB. Exiting" % tape.label)
            sys.exit(2)

    tapeids = {t.id for t in tapes}

    if(options.nodedup):
        logger.info("Nodeduplicate option given - not skipping files already on any of these tapes")
    else:
        logger.info("Checking for duplication on these tapes")
        actual_diskfiles = []
        for df in diskfiles:
            query = (session.query(Tape.id).select_from(join(TapeFile, join(TapeWrite, Tape)))
                        .filter(Tape.active == True)
                        .filter(TapeWrite.suceeded == True)
                        .filter(TapeFile.filename == df.filename)
                        .filter(TapeFile.md5 == df.file_md5))

            mytapeids = query.all()
            if len(mytapeids)==0:
                # this file is not on any tapes, include it
                logger.debug("File not on any tapes, not de-duping it")
                actual_diskfiles.append(df)
            else:
                # Need to loop through the tapes that the file is on (mytapeids) and see if any of them
                # are in the tapes in the drives (tapeids). If so, ditch the file
                for (mtid,) in mytapeids:
                    if mtid in tapeids:
                        autolog("File %s is on one of the tapes we have, skipping it" % df.filename)
                        # Ditch the file
                        break
                else:
                    actual_diskfiles.append(df)

        diskfiles = actual_diskfiles

    if options.skip:
        logger.info("Checking for duplication to any tapes")
        actual_diskfiles = []
        for df in diskfiles:
            query = (session.query(TapeFile).select_from(join(TapeFile, join(TapeWrite, Tape)))
                        .filter(Tape.active == True)
                        .filter(TapeWrite.suceeded == True)
                        .filter(TapeFile.filename == df.filename)
                        .filter(TapeFile.md5 == df.file_md5))

            num = query.count()
            if num == 0:
                actual_diskfiles.append(df)
                logger.debug("Not skipping file %s as it is on 0 tapes" % df.filename)
            else:
                autolog("Skipping File %s : is already on tape %d times" % (df.filename, num))

        diskfiles = actual_diskfiles

    numfiles = len(diskfiles)
    totalsize = 0
    for df in diskfiles:
        totalsize += df.file_size

    logger.info("Got %d files totalling %.2f GB to write to tape" % (numfiles, (totalsize / 1.0E9)))
    if(numfiles == 0):
        logger.info("Exiting - no files")
        exit(0)

    # Check the tape label in the drives
    if(not options.dontcheck):
        for tapelabel, td in zip(options.tapelabel, tds):
            logger.info("Checking tape label in drive %s" % td.dev)
            if(td.online() == False):
                logger.error("No tape in drive %s" % td.dev)
                session.close()
                sys.exit(1)
            thislabel = td.readlabel()
            if(thislabel != tapelabel):
                logger.error("Label of tape in drive %s: %s does not match label given as %s" % (td.dev, thislabel, tapelabel))
                session.close()
                sys.exit(1)
            logger.info("OK - found tape in drive %s with label: %s" % (td.dev, thislabel))

    # check md5s match for what's on disk.
    logger.info("Verifying md5s")
    for df in diskfiles:
        actual_md5 = df.get_file_md5()
        db_md5 = df.file_md5
        if(actual_md5 != db_md5):
            logger.error("md5sum mismatch for file %s: file: %s, database: %s" % (df.filename, actual_md5, db_md5))
            session.close()
            sys.exit(1)
 
    logger.info("All files fetched OK")

    # Now loop through the tapes, doing all the stuff on each
    oldcwd = os.getcwd()
    os.chdir(storage_root)
    for td, tape in zip(tds, tapes):
        logger.debug("About to write on tape label %s in drive %s" % (tape.label, td.dev))
        # Position Tape
        if(not options.dryrun):
            logger.info("Positioning Tape %s" % td.dev)
            td.setblk0()
            td.eod(fail=True)

            if td.eot():
                logger.error("Tape %s in %s is at End of Tape. Tape is Full. Marking tape as full in DB and aborting" % (tape.label, td.dev))
                tape.full = True
                session.commit()
                td.cleanup()
                session.close()
                sys.exit(1)

            # Update tape first/lastwrite
            logger.debug("Updating tape record for tape label %s" % tape.label)
            if tape.firstwrite is None:
                tape.firstwrite = datetime.datetime.utcnow()
            tape.lastwrite = datetime.datetime.utcnow()
            session.commit()

            # Create tapewrite record
            logger.debug("Creating TapeWrite record for tape %s" % tape.label)
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
            tw.suceeded = False
            session.commit()

            # Write the tape.
            bytecount = 0
            blksize = 64 * 1024

            logger.info("Creating tar archive on tape %s on drive %s" % (tape.label, td.dev))
            try:
                mode = td.write_mode()
                tar = tarfile.open(name=td.target(), mode=mode, bufsize=blksize)
                tarok = True
            except:
                logger.error("Error opening tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                tarok = False

            backup_files = list()
            for df in diskfiles:
                filename = df.filename
                logger.info("Adding %s to tar file on tape %s in drive %s" % (filename, tape.label, td.dev))
                try:
                # the filename is a unicode string, and tarfile cannot handle this, convert to ascii
                    #filename = filename.encode('ascii')
                    if not options.compress or filename.lower().endswith(".bz2"):
                        # tar.add(filename)
                        backup_files.append(BackupFile(filename, "%s/%s" % (storage_root, filename), df))
                    else:
                        # we have to stage a bzip2 file to send to the tar, plus calculate it's md5/size
                        f = open(filename, "rb")
                        bzip_filename = "%s.bz2" % filename
                        stagefilename = mkstemp(dir=td.workingdir)[1]
                        bzf = BZ2File(stagefilename, "w")
                        bzf.write(f.read())
                        bzf.flush()
                        bzf.close()
                        f.close()

                        backup_files.append(BackupFile(bzip_filename, stagefilename, df))
                except:
                    logger.error("Error queueing file for tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                    tarok = False
                    break

            for backup_file in backup_files:
                try:
                    statinfo = os.stat(backup_file.stagefilename)
                    f = open(backup_file.stagefilename, "rb")
                    tarinfo = tarfile.TarInfo(backup_file.bz2_filename)
                    tarinfo.size = statinfo.st_size  # tarinfo.size
                    tarinfo.mtime = statinfo.st_mtime
                    tarinfo.mode = statinfo.st_mode
                    tarinfo.type = tarfile.REGTYPE
                    tarinfo.uid = statinfo.st_uid
                    tarinfo.gid = statinfo.st_gid
                    # tarinfo.uname = statinfo.
                    # tarinfo.gname = tarinfo.gname

                    tar.addfile(tarinfo, f)
                    f.close()
                    # Create the TapeFile entry and add to DB
                    tapefile = TapeFile()
                    tapefile.tapewrite_id = tw.id
                    tapefile.filename = backup_file.disk_file.filename
                    tapefile.md5 = backup_file.disk_file.file_md5
                    tapefile.size = int(backup_file.disk_file.file_size)
                    tapefile.lastmod = backup_file.disk_file.lastmod
                    tapefile.compressed = backup_file.disk_file.compressed
                    tapefile.data_size = backup_file.disk_file.data_size
                    tapefile.data_md5 = backup_file.disk_file.data_md5
                    session.add(tapefile)
                    session.commit()
                    bytecount += int(df.file_size)
                except:
                    logger.error("Error adding file to tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                    logger.info("Probably the tape filled up - Marking tape as full in the DB - label: %s" % tape.label)
                    tape.full = True
                    session.commit()
                    tarok = False
                    break

            logger.info("Completed writing tar archive on tape %s in drive %s" % (tape.label, td.dev))
            logger.info("Wrote %d bytes = %.2f GB" % (bytecount , (bytecount/1.0E9)))
            try:
                tar.close()
            except:
                logger.error("Error closing tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                tarok = False

            # update records post-write
            logger.debug("Updating tapewrite record")
            tw.enddate = datetime.datetime.utcnow()
            logger.debug("Succeeded: %s" % tarok)
            tw.suceeded = tarok
            tw.afterstatus = td.status()
            tw.size = bytecount
            session.commit()

os.chdir(oldcwd)
session.close()
logger.info("*** write_to_tape exiting normally at %s" % datetime.datetime.now())
