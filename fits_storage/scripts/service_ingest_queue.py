#! /usr/bin/env python
from fits_storage.orm import session_scope
from fits_storage.orm.ingestqueue import IngestQueue
from fits_storage.fits_storage_config import using_s3, storage_root, fits_lockfile_dir, export_destinations
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.utils.exportqueue import ExportQueueUtil
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.utils.pidfile import PidFile, PidFileError
import signal
import sys
import os
import datetime
import time
import traceback
from sqlalchemy.exc import OperationalError

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--skip-fv", action="store_true", dest="skip_fv", default=False, help="Do not run fitsverify on the files")
parser.add_option("--skip-wmd", action="store_true", dest="skip_wmd", default=False, help="Do not run a wmd check on the files")
parser.add_option("--no-defer", action="store_true", dest="no_defer", default=False, help="Do not defer ingestion of recently modified files")
parser.add_option("--fast-rebuild", action="store_true", dest="fast_rebuild", default=False, help="Fast rebuild mode - skip duplication checking etc")
parser.add_option("--make-previews", action="store_true", dest="make_previews", default=False, help="Make previews during ingest rather than queueing for later")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--name", action="store", dest="name", default="service_ingest_queue", help="Name for this process. Used in logfile and lockfile")
parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")
parser.add_option("--empty", action="store_true", default=False, dest="empty", help="This flag indicates that service ingest queue should empty the current queue and then exit.")
options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)
if options.name:
    setlogfilesuffix(options.name)

# Need to set up the global loop variable before we define the signal handlers
# This is the loop forever variable later, allowing us to stop cleanly via kill
global loop
loop = True

# Define signal handlers. This allows us to bail out neatly if we get a signal
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

# Announce startup
logger.info("*********    service_ingest_queue.py - starting up at %s", datetime.datetime.now())

try:
    with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, session_scope() as session:
        ingest_queue = IngestQueueUtil(session, logger, skip_fv = options.skip_fv,
                                                        skip_md = options.skip_wmd,
                                                        make_previews = options.make_previews)
        export_queue = ExportQueueUtil(session, logger)

        # Loop forever. loop is a global variable defined up top
        while loop:
            try:
                # Request a queue entry
                iq = ingest_queue.pop(options.fast_rebuild)

                if iq is None:
                    if options.empty:
                        logger.info("Nothing on queue and --empty flag set, exiting")
                        break
                    else:
                        logger.info("Nothing on queue... Waiting")
                    time.sleep(2)
                else:
                    # Don't query queue length in fast_rebuild mode
                    if options.fast_rebuild:
                        logger.info("Ingesting %s", iq.filename)
                    else:
                        logger.info("Ingesting %s, (%d in queue)", iq.filename, ingest_queue.length())

                    # Check if the file was very recently modified or is locked, defer ingestion if it was
                    if not (using_s3 or options.no_defer):
                        if ingest_queue.maybe_defer(iq):
                            continue

                    try:
                        added_diskfile = ingest_queue.ingest_file(iq.filename, iq.path, iq.force_md5, iq.force)
                        # Now we also add this file to our export list if we have downstream servers and we did add a diskfile
                        if added_diskfile:
                            for destination in export_destinations:
                                export_queue.add_to_queue(iq.filename, iq.path, destination)
                    except:
                        logger.info("Problem Ingesting File - Rolling back")
                        logger.error("Exception ingesting file %s: %s : %s... %s",
                                     iq.filename, sys.exc_info()[0], sys.exc_info()[1],
                                                  traceback.format_tb(sys.exc_info()[2]))
                        # We leave inprogress as True here, because if we set it back to False, we get immediate retry and rapid failures
                        # iq.inprogress=False

                        # Recover the session to a working state and log the error to the database
                        ingest_queue.set_error(iq, *sys.exc_info())
                        raise
                    logger.debug("Deleting ingestqueue id %d", iq.id)
                    ingest_queue.delete(iq)

            except (KeyboardInterrupt, OperationalError):
                loop = False

            except:
                string = traceback.format_tb(sys.exc_info()[2])
                string = "".join(string)
                session.rollback()
                logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
                # Press on with the next file, don't raise the exception further.
                # raise
except PidFileError as e:
    logger.error(str(e))

logger.info("*********    service_ingest_queue.py - exiting at %s", datetime.datetime.now())
