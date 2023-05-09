#! /usr/bin/env python3

import datetime
import signal
import time
from argparse import ArgumentParser

from sqlalchemy.exc import OperationalError

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.db import session_scope
from fits_storage.queues.queue.fileopsqueue import FileopsQueue, FileOpsResponse

from fits_storage.server.fileopser import FileOpser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--debug", action="store_true", dest="debug",
                        default=False, help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        default=False,
                        help="Run as background demon, do not generate stdout")

    parser.add_argument("--name", action="store", dest="name",
                        help="Name for this instance of this task. "
                             "Used in logfile and lockfile")

    parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                        help="Use a lockfile to limit instances")

    parser.add_argument("--empty", action="store_true", dest="empty",
                        default=False,
                        help="Exit once the queue is empty.")

    parser.add_argument("--oneshot", action="store_true", dest="oneshot",
                        default=False, help="Process only one file then exit")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.name is not None:
        setlogfilesuffix(options.name)

    # Need to set up the global loop variable before we define the signal
    # handlers This is the loop forever variable later, allowing us to stop
    # cleanly via kill
    loop = True

    # Define signal handlers. This allows us to bail out cleanly e.g. if we get
    # a signal. These need to be defined after logger is set up as there is no
    # way to pass the logger as an argument to these.
    def handler(signum, frame):
        logger.error("Received signal: %d. Crashing out.", signum)
        raise KeyboardInterrupt('Signal', signum)

    def nicehandler(signum, frame):
        logger.error("Received signal: %d. Attempting to stop nicely.", signum)
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
    logger.info("***   service_fileops_queue.py - starting up at %s",
                datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    try:
        with PidFile(logger, name=options.name, dummy=not options.lockfile) as pidfile, \
                session_scope() as session:

            fileops_queue = FileopsQueue(session, logger)
            fileopser = FileOpser(session, logger)

            # Loop forever. loop is a global variable defined up top
            while loop:
                try:
                    # Request a queue entry. The returned entry is marked
                    # as inprogress and committed to the session.
                    fqe = fileops_queue.pop()

                    if fqe is None:
                        if options.empty:
                            logger.info("Nothing on queue and "
                                        "--empty flag set, exiting")
                            break
                        else:
                            logger.info("Nothing on queue... Waiting")
                            time.sleep(1)
                            continue

                    if options.oneshot:
                        loop = False

                    # Process the queue entry.
                    # .fileop() is basically responsible for everything else
                    # from here
                    fileopser.fileop(fqe)

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    # fileop() should handle its own exceptions, and
                    # should never raise, so if there's a problem with a given
                    # entry we log the error and continue with the next one.
                    # This catches anything else in the code above. Again, we
                    # log the error and carry on. Probably the error would
                    # reoccur if we re-try the same entry though, so we set it
                    # as failed and record the error in the fqe too.
                    message = "Unknown Error - no FileopsQueueEntry instance"
                    if fqe is not None:
                        fqe.failed = True
                        fqe.inprogress = False
                        message = "Exception in service_fileops_queue while " \
                                  f"processing FQE id {fqe.id}"
                        fqe.error = message
                        session.commit()

                    logger.error(message, exc_info=True)
                    # Press on with the next file, don't raise the exception

    except PidFileError as e:
        logger.error(str(e))

    logger.info("***    service_fileops_queue.py - exiting at "
                f"{datetime.datetime.now()}")
