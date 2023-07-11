#!/usr/bin/env python3

import sys
import signal
import datetime
import tarfile
from sqlalchemy.exc import NoResultFound
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile
from fits_storage.server.tapeutils import TapeDrive

from fits_storage.db import session_scope
from fits_storage.core.hashes import md5sum_size_fp

from fits_storage.config import get_config
fsc = get_config()

# Option Parsing
parser = OptionParser()
parser.add_option("--tapedrive", action="store", type="string",
                  default="/dev/nst0", dest="tapedrive",
                  help="tapedrive to use.")
parser.add_option("--verbose", action="store_true", dest="verbose",
                  help="Log message to say 'this file is OK' for the files that"
                       " are OK, as opposed to the normal mode where we only "
                       "log something when there is a problem...")
parser.add_option("--start-from", action="store", type="int", dest="start",
                  help="Start from file number instead of beginning of tape")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Make separate log files per tape drive
setlogfilesuffix(options.tapedrive.split('/')[-1])


# Define signal handlers. This allows us to ingore some signals in demon mode.
# These need to be defined after logger is set up as there is no way to pass
# the logger as an argument to these.
def handler(signum, frame):
    logger.warning("Received signal: %d. Ignoring...", signum)
if options.demon:
    signal.signal(signal.SIGHUP, handler)

# Announce startup
logger.info("***   verify_tape.py - starting up at %s", datetime.datetime.now())

with session_scope() as session:
    td = TapeDrive(options.tapedrive, fsc.fits_tape_scratchdir)
    td.setblk0()
    label = td.readlabel()
    logger.debug('Read tape label: %s', label)

    # Find the tape in the DB
    try:
        tape = session.query(Tape).filter(Tape.active==True)\
            .filter(Tape.label==label).one()
    except NoResultFound:
        logger.error("The tape %s was not found in the DB.", label)
        sys.exit(1)

    # A flag so that we can say at the end you need to look back at the logfile.
    error_happened = False

    # Find all the tapewrite objects for it, loop through them in filenum order
    tw_list = session.query(TapeWrite).filter(TapeWrite.succeeded == True)\
        .filter(TapeWrite.tape_id==tape.id).order_by(TapeWrite.filenum).all()
    for tw in tw_list:
        errors_this_file = False
        if options.start:
            # Skip over tapewrites until we get to start-from file number
            if tw.filenum < options.start:
                logger.debug("Skipping file number %d which is before "
                             "--start-from position", tw.filenum)
                continue
        # Send the tapedrive to this tapewrite
        try:
            logger.debug("Sending tape to filenumber %d", tw.filenum)
            td.skipto(filenum=tw.filenum, fail=True)
        except IOError:
            logger.error("File number in the database but not on tape - "
                         "at filenum: %s", tw.filenum)
            errors_this_file = True
            break
        logger.info("Reading the files from tape: %s, at file number: %d",
                    label, tw.filenum)

        # Read all the fits files in the tar archive, one at a time,
        # looping through and calculating the md5
        files_on_tape = []
        block = 64*1024
        tdfileobj = None
        try:
            # We have to open the tape drive manually, so that we can close it
            # ourselves if the tar header read fails
            tdfileobj = open(td.dev, 'rb')
            tar = tarfile.open(name=td.dev, fileobj=tdfileobj, mode='r|',
                               bufsize=block)
            for tar_info in tar:
                filename = tar_info.name
                compressed = filename.lower().endswith(".bz2")
                logger.debug("Found file %s on tape.", filename)

                # Find the tapefile object
                try:
                    tf = session.query(TapeFile)\
                        .filter(TapeFile.tapewrite_id==tw.id)\
                        .filter(TapeFile.filename==filename).one()
                except NoResultFound:
                    logger.error("File %s on tape but not found in DB - at "
                                 "filenum %d.", filename, tw.filenum)
                    errors_this_file = True
                    continue

                files_on_tape.append(filename)
                # Compare the tapefile record in the DB and the tarinfo object
                # for the actual file on tape
                if tar_info.size != tf.size:
                    logger.error("Size mismatch between tar_info and in DB for "
                                 "file %s in filenum %d. tar_info size: %d, "
                                 "DB size: %d", tf.filename, tw.filenum,
                                                 tar_info.size, tf.size)
                    errors_this_file = True
                # Calculate the md5 of the data on tape
                f = tar.extractfile(tar_info)
                try:
                    size, md5 = md5sum_size_fp(f)
                except Exception:
                    logger.error("Error reading data from tar file. Likely this"
                                 " is a tape error. Filename: %s in filenum %d",
                                 tf.filename, tw.filenum)
                    errors_this_file = True
                    break
                f.close()

                if size != tf.size:
                    logger.error("Size mismatch between tape and DB for file: "
                                 "%s, in filenum: %d. On-tape size: %d, "
                                 "db-size: %d", tf.filename, tw.filenum,
                                 size, tf.size)
                    errors_this_file = True
                if md5 != tf.md5:
                    logger.error("md5 mismatch between tape and DB for file: "
                                 "%s, in filenum: %d. On tape md5: %s, "
                                 "db md5: %s", tf.filename, tw.filenum,
                                 md5, tf.md5)
                    errors_this_file = True
            tar.close()

        except tarfile.ReadError:
            logger.warning("Tar read error on open - most likely an empty tar "
                           "file: tape %s, file number %d", label, tw.filenum)
        finally:
            if tdfileobj is not None:
                tdfileobj.close()

            if errors_this_file:
                logger.debug("There were errors while reading this file, "
                             "not checking for completeness")
                error_happened = True
            else:
                # Check whether we read everything in the DB
                files_in_DB = session.query(TapeFile)\
                    .filter(TapeFile.tapewrite_id==tw.id)\
                    .order_by(TapeFile.filename).all()
                for dbfile in files_in_DB:
                    if dbfile.filename not in files_on_tape:
                        logger.error("This file was in the database, but not"
                                     " on the tape: %s, in filenum: %d",
                                     dbfile.filename, tw.filenum)
                        error_happened = True

    # If there were errors, call attention to that
    if error_happened:
        logger.error("There were verify errors - please see logfile for "
                     "details. Not updating lastverified")
    else:
        now = datetime.datetime.utcnow()
        logger.info("There were no verify errors - updating lastverified "
                    "to: %s UTC" % now)
        tape.lastverified = now
        session.commit()

logger.info("***   verify_tape.py - exiting at %s", datetime.datetime.now())
