#! /usr/bin/env python
import signal
import sys
import os
import datetime
import time
import traceback
from sqlalchemy.exc import OperationalError
from optparse import OptionParser

from fits_storage.orm.target import TargetQueue, TargetsChecked
from fits_storage.fits_storage_config import fits_lockfile_dir
from fits_storage.utils.targetqueue import TargetQueueUtil
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.utils.pidfile import PidFile, PidFileError

from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile




if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
    parser.add_option("--name", action="store", dest="name", help="Name for this process. Used in logfile and lockfile")
    parser.add_option("--lockfile", action="store_true", default="service_target_queue", dest="lockfile", help="Use a lockfile to limit instances")
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
    logger.info("*********    service_target_queue.py - starting up at %s", datetime.datetime.now())

    try:
        with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, session_scope() as session:
            target_queue = TargetQueueUtil(session, logger)
            # Loop forever. loop is a global variable defined up top
            while loop:
                try:
                    # Request a queue entry
                    tq = target_queue.pop()

                    if tq is None:
                        logger.info("Nothing on queue.")
                        if options.empty:
                            logger.info("--empty flag set, exiting")
                            break
                        else:
                            logger.debug("...Waiting")
                        time.sleep(5)
                    else:

                        try:
                            # Actually make the preview here
                            target_queue.process(tq, make=True)
                        except:
                            logger.info("Problem Making Target List - Rolling back")
                            logger.error("Exception making target list %s: %s : %s... %s" %
                                         (tq.diskfile_id, sys.exc_info()[0], sys.exc_info()[1],
                                                         traceback.format_tb(sys.exc_info()[2])))
                            session.rollback()
                            # We leave inprogress as True here, because if we set it back to False, we get immediate retry and rapid failures
                            # tq.inprogress=False
                            # Recover the session to a working state and log the error to the database
                            target_queue.set_error(tq, *sys.exc_info())
                            raise
                        logger.debug("Deleting targetqueue id %d", tq.id)
                        target_queue.delete(tq)

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    string = "".join(traceback.format_tb(sys.exc_info()[2]))
                    session.rollback()
                    logger.error("Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
                    # Press on with the next file, don't raise the exception further.
                    # raise
    except PidFileError as e:
        logger.error(str(e))

    logger.info("*********    service_target_queue.py - exiting at %s", datetime.datetime.now())
