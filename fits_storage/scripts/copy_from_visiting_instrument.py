#! /usr/bin/env python3

import signal
import datetime
import time
from optparse import OptionParser


from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.visitor_instrument_helper import AlopekeVIHelper, \
    ZorroVIHelper, IGRINSVIHelper
from fits_storage.queues.queue.ingestqueue import IngestQueue
from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry

from fits_storage.config import get_config

from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile


# Utility functions
def check_present(session, filename):
    """
    Check if the given filename is present in the database.

    This method checks the file against the `fits_storage.orm.DiskFile` table to
    see if it already exists and is marked present.  It differs slightly from
    the DHS version of this logic in that it will look for both a `.bz2` and
    non-`.bz2` variant of the file.

    We also check if the file is waiting in the ingest queue, and return True
    if so.

    Parameters
    ----------

    session : `sqlalchemy.orm.session.Session`
        SQLAlchemy session to check against
    filename : str
        Name of the file to look for

    Returns
    -------
        True if a record exists in `fits_storage.orm.DiskFile` for this
        filename with `present` set to True
    """
    otherfilename = filename
    if otherfilename.endswith('.bz2'):
        otherfilename = otherfilename[:-4]
    else:
        otherfilename = "%s.bz2" % otherfilename

    for fn in (filename, otherfilename):
        df = session.query(DiskFile).filter(DiskFile.filename == fn).\
            filter(DiskFile.canonical == True).first()
        if df:
            return True
        iqe = session.query(IngestQueueEntry). \
            filter(IngestQueueEntry.inprogress == False). \
            filter(IngestQueueEntry.filename == fn).first()
        if iqe:
            return True

    return False


# Option Parsing
parser = OptionParser()
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Don't actually do anything")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run in background mode")
parser.add_option("--force", action="store_true", dest="force",
                  help="Copy file even if already present on disk")
parser.add_option("--alopeke", action="store_true", dest="alopeke",
                  default=False, help="Copy Alopeke data")
parser.add_option("--zorro", action="store_true", dest="zorro",
                  default=False, help="Copy Zorro data")
parser.add_option("--igrins", action="store_true", dest="igrins",
                  default=False, help="Copy IGRINS data")
parser.add_option("--datepre", action="store", dest="datepre", default=None,
                  help="Date prefix to filter directory names on")
parser.add_option("--onepass", action="store_true",
                  help="Perform a single pass rather than looping indefinately")
parser.add_option("--noqueue", action="store_true",
                  help="Do not add copied files to ingest queue")
(options, args) = parser.parse_args()

# Logging level to debug?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***  copy_from_visiting_instrument.py - starting up at %s"
            % datetime.datetime.now())

# Need to set up the global loop variable before we define the signal
# handlers. This is the loop forever variable later, allowing us to stop
# cleanly via kill
loop = True

# Define signal handlers to allow us to bail out neatly if we get a signal


def handler(signum, frame):
    logger.error("Received signal: %d. Crashing out. ", signum)
    raise KeyboardInterrupt('Signal', signum)


def nicehandler(signum, frame):
    logger.error("Received signal: %d. Attempting to stop nicely ", signum)
    global loop
    loop = False


# Set handlers for the signals we want to handle
# Cannot trap SIGKILL or SIGSTOP, all others are fair game
signal.signal(signal.SIGHUP, nicehandler)
signal.signal(signal.SIGINT, nicehandler)
signal.signal(signal.SIGQUIT, nicehandler)
signal.signal(signal.SIGILL, handler)
signal.signal(signal.SIGABRT, handler)
signal.signal(signal.SIGFPE, handler)
signal.signal(signal.SIGSEGV, handler)
signal.signal(signal.SIGPIPE, handler)
signal.signal(signal.SIGTERM, nicehandler)

fsc = get_config()
if fsc.using_s3:
    logger.info("This should not be used with S3 storage. Exiting")
    exit(1)
if options.demon and options.force:
    logger.info("Force not not available when running as daemon")
    exit(2)
if int(options.alopeke) + int(options.zorro) + int(options.igrins) != 1:
    logger.info("You must supply exactly one of alopeke, zorro or igrins")
    exit(3)

# Get the VI Helper instance
vihelper = None
if options.alopeke:
    vihelper = AlopekeVIHelper(logger=logger)
elif options.zorro:
    vihelper = ZorroVIHelper(logger=logger)
elif options.igrins:
    vihelper = IGRINSVIHelper(logger=logger)


with session_scope() as session:
    iq = IngestQueue(session, logger=logger) if not options.noqueue else None
    while loop:
        if options.onepass:
            loop = False
        # Did we actually do anything this pass?
        did_something = False
        # Loop over date directories:
        for datedir in vihelper.list_datedirs():
            if options.datepre and not datedir.startswith(options.datepre):
                logger.debug("Skipping date dir %s as doesn't match datepre",
                             datedir)
                continue

            vihelper.subdir = datedir
            # Loop over matching files in this datedir
            for filename in vihelper.list_files():
                if not options.force and vihelper.file_exists(filename):
                    logger.debug("File %s already exists on destination, "
                                 "skipping", filename)
                    continue

                # If we got here, there was at least one actionable file
                # this pass
                did_something = True

                # If lastmod time is within 5 secs, it may be still being
                # written to, skip it
                if vihelper.too_new(filename):
                    continue

                if options.dryrun:
                    logger.info("Dry run, not actually copying %s", filename)
                    continue

                logger.info("Copying %s", filename)
                if vihelper.fix_and_copy(filename):
                    logger.debug("Copy appeared to succeed")
                    # Add to ingest queue?
                    if not options.noqueue:
                        path = f"{vihelper.instrument_name.lower()}/" \
                               f"{vihelper.subdir}"
                        logger.info("Adding %s in %s to ingest queue",
                                    filename, path)
                        iq.add(filename, path)
                        session.commit()
                else:
                    logger.debug("Copy failed")

        # If we didn't do anything, wait a while before looping
        if loop and not did_something:
            logger.info("No action taken this pass, waiting 60 secs")
            time.sleep(60)
