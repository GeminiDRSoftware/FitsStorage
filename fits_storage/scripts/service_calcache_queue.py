#! /usr/bin/env python3

import signal
import sys
import datetime
import time
import traceback

from sqlalchemy.exc import OperationalError

from optparse import OptionParser

from fits_storage.db import session_scope
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.queues.queue import CalCacheQueue


from fits_storage.config import get_config
fsc = get_config()

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--fast-rebuild", action="store_true",
                      dest="fast_rebuild", default=False,
                      help="Fast rebuild mode - skip duplication checking etc")
    parser.add_option("--debug", action="store_true", dest="debug",
                      default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      default=False, help="Run as a background demon, do not generate stdout")
    parser.add_option("--name", action="store", dest="name",
                      help="Name for this process. Used in logfile and lockfile")
    parser.add_option("--lockfile", action="store_true", dest="lockfile",
                      help="Use a lockfile to limit instances")
    parser.add_option("--empty", action="store_true", default=False,
                      dest="empty", help="Exit once the queue is empty.")
    parser.add_option("--oneshot", action="store_true", default=False,
                      dest="oneshot", help="Process one queue entry then exit")
    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.name:
        setlogfilesuffix(options.name)

    # Need to set up the global loop variable before we define the signal
    # handlers. This is the loop forever variable later, allowing us to stop
    # cleanly via kill
    global loop
    loop = True

    # Define signal handlers. This allows us to exit cleanly if we get a signal
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
    logger.info("***    service_calcache_queue.py - starting up at %s",
                datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    try:
        with PidFile(logger, name=options.name, dummy=not options.lockfile) as pidfile, \
                session_scope() as session:

            # Loop forever. loop is a global variable defined up top
            ccq = CalCacheQueue(session, logger)
            while loop:
                try:
                    # Request a queue entry
                    ccqe = ccq.pop()

                    if ccqe is None:
                        if options.empty:
                            logger.info("Nothing on queue, and "
                                        "--empty flag set, exiting")
                            break
                        else:
                            logger.info("Nothing on queue. Waiting...")
                            time.sleep(10)
                            continue

                    # Don't query queue length in fast_rebuild mode
                    if options.fast_rebuild:
                        logger.info("Processing obs_hid %d - %s" %
                                    ccqe.obs_hid, ccqe.filename)
                    else:
                        logger.info("Processing obs_hid %d - %s, (%d in queue)"
                                    % ccqe.obs_hid, ccqe.filename, ccq.length())

                    try:
                        # Do the associations and put them in the CalCache table
                        success = True
                        ccq.cache_associations(ccqe.obs_hid)
                    except:
                        success = False
                        session.rollback()
                        message = "Exception while associating calibrations " \
                                  "for CalCache"
                        logger.info(message, exc_info=True)
                        ccqe.inprogress = False
                        ccqe.failed = True
                        ccqe.error = message
                        session.commit()

                    if success:
                        logger.debug("Deleting calcachequeue id %d" % ccqe.id)
                        session.delete(ccqe)

                    if options.oneshot:
                        loop = False

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    session.rollback()
                    logger.error("Exception", exc_info=True)
                    # Press on with the next file, don't raise the exception
                    # further.

    except PidFileError as e:
        logger.error(str(e))

    logger.info("***    service_calcache_queue.py - exiting at %s",
                datetime.datetime.now())
