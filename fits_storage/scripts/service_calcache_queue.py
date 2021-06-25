#! /usr/bin/env python
from gemini_obs_db import session_scope
from fits_storage.orm.calcachequeue import CalCacheQueue
from fits_storage.utils.calcachequeue import CalCacheQueueUtil, cache_associations
from fits_storage.fits_storage_config import fits_lockfile_dir
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


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--fast-rebuild", action="store_true", dest="fast_rebuild", default=False, help="Fast rebuild mode - skip duplication checking etc")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
    parser.add_option("--name", action="store", dest="name", default="service_calcache_queue", help="Name for this process. Used in logfile and lockfile")
    parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")
    parser.add_option("--empty", action="store_true", default=False, dest="empty", help="This flag indicates that we should empty the current queue and then exit.")
    (options, args) = parser.parse_args()

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

    # Annouce startup
    logger.info("*********    service_calcache_queue.py - starting up at %s", datetime.datetime.now())

    try:
        with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, session_scope() as session:
            # Loop forever. loop is a global variable defined up top
            ccq_util = CalCacheQueueUtil(session, logger)
            while loop:
                try:
                    # Request a queue entry
                    ccq = ccq_util.pop(options.fast_rebuild)

                    if ccq is None:
                        logger.info("Nothing on queue.")
                        if options.empty:
                            logger.info("--empty flag set, exiting")
                            break
                        else:
                            logger.info("...Waiting")
                        time.sleep(10)
                    else:
                        # Don't query queue length in fast_rebuild mode
                        if options.fast_rebuild:
                            logger.info("Processing obs_hid %d" % ccq.obs_hid)
                        else:
                            logger.info("Processing obs_hid %d, (%d in queue)" % (ccq.obs_hid, ccq_util.length()))

                        try:
                            # Do the associations and put them in the CalCache table
                            cache_associations(session, ccq.obs_hid)

                        except:
                            logger.info("Problem Associating Calibrations for Cache - Rolling back")
                            logger.error("Exception processing obs_hid %d: %s : %s... %s" % (ccq.obs_hid, sys.exc_info()[0], sys.exc_info()[1], traceback.format_tb(sys.exc_info()[2])))
                            # We leave inprogress as True here, because if we set it back to False, we get immediate retry and rapid failures
                            # iq.inprogress=False

                            # Recover the session to a working state and log the error to the database
                            ccq_util.set_error(ccq, *sys.exc_info())
                            raise
                        logger.debug("Deleteing calcachequeue id %d" % ccq.id)
                        # ccq is a transient ORM object, find it in the db
                        ccq_util.delete(ccq)

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    string = traceback.format_tb(sys.exc_info()[2])
                    string = "".join(string)
                    session.rollback()
                    logger.error("Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
                    # Press on with the next file, don't raise the esception further. Unless if debugging uncomment next line
                    # raise
    except PidFileError as e:
        logger.error(str(e))

    logger.info("*********    service_calcache_queue.py - exiting at %s", datetime.datetime.now())
