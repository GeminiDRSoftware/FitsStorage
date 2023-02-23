#! /usr/bin/env python

import datetime
import signal
import sys
import time
import traceback

from sqlalchemy.exc import OperationalError

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.db import session_scope
from fits_storage.queues.queue import IngestQueue

from fits_storage.config import get_config
fsc = get_config()


if __name__ == "__main__":

    # ------------------------------------------------------------------------------
    parser = ArgumentParser()
    parser.add_argument("--skip-fv", action="store_true", dest="skip_fv",
                        default=False, help="Do not fitsverify the files")

    parser.add_argument("--skip-wmd", action="store_true", dest="skip_wmd",
                        default=False, help="Do not metadata check the files")

    parser.add_argument("--no-defer", action="store_true", dest="no_defer",
                        default=False,
                        help="Do not defer ingest of recently modified files")

    parser.add_argument("--fast-rebuild", action="store_true",
                        dest="fast_rebuild", default=False,
                        help="Fast rebuild mode - various optimizations for"
                             "bulk rebuilding, eg skip duplication checking.")

    parser.add_argument("--make-previews", action="store_true",
                        dest="make_previews", default=False,
                        help="Make previews during ingest rather than adding to"
                             "preview queue. This is more efficient overall but"
                             "slows down the actual ingest phase")

    parser.add_argument("--debug", action="store_true", dest="debug",
                        default=False, help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        default=False,
                        help="Run as background demon, do not generate stdout")

    parser.add_argument("--name", action="store", dest="name",
                        default="",
                        help="Name for this instance of this task. "
                             "Used in logfile and lockfile")

    parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                        help="Use a lockfile to limit instances")

    parser.add_argument("--empty", action="store_true", dest="empty",
                        default=False,
                        help="This flag indicates that service_ingest_queue "
                             "should exit once the queue is empty.")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    taskname = 'service_ingest_queue'
    if options.name:
        taskname += '-' + options.name
        setlogfilesuffix(taskname)

    # Need to set up the global loop variable before we define the signal
    # handlers This is the loop forever variable later, allowing us to stop
    # cleanly via kill
    loop = True

    # Define signal handlers. This allows us to bail out cleanly e.g. if we get
    # a signal. These need to be defined after logger is set up as there is no
    # way to pass the logger as an argument to these.
    def handler(signum, frame):
        logger.error(f"Received signal: {signum}. Crashing out.")
        raise KeyboardInterrupt('Signal', signum)

    def nicehandler(signum, frame):
        logger.error(f"Received signal: {signum}. Attempting to stop nicely.")
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
    logger.info("***    service_ingest_queue.py - starting up at "
                f"{datetime.datetime.now()}")

    try:
        with PidFile(taskname, logger, dummy=not options.lockfile) as pidfile, \
                session_scope() as session:

            ingest_queue = IngestQueue(session, logger)

            # Loop forever. loop is a global variable defined up top
            while loop:
                try:
                    # Request a queue entry. The returned entry is marked
                    # as inprogress and committed to the session.
                    iqe = ingest_queue.pop()

                    if iqe is None:
                        if options.empty:
                            logger.info("Nothing on queue and "
                                        "--empty flag set, exiting")
                            break
                        else:
                            logger.info("Nothing on queue... Waiting")
                            time.sleep(2)
                    else:
                        # Don't query queue length in fast_rebuild mode
                        if options.fast_rebuild:
                            logger.info(f"Ingesting {iqe.filename} - "
                                        f"id {iqe.id}")
                        else:
                            logger.info(f"Ingesting {iqe.filename} - "
                                        f"id {iqe.id} "
                                        f" ({ingest_queue.length()} in queue)")

                        # Check if the file was very recently modified or is
                        # locked, defer ingestion if so
                        if not (fsc.using_s3 or options.no_defer):
                            defer_message = iqe.defer
                            if defer_message is not None:
                                logger.info(defer_message)
                                session.commit()
                                continue

                        # Go ahead and ingest the file. ingest_file(iqe) is also
                        # responsible for adding the file to the export queue
                        # if appropriate. At this point, iqe is marked as
                        # inprogress and is committed to the database.
                        # ingest_file(iqe) should delete it if it successfully
                        # ingests it, or should set the failed and error states
                        # and commit it if not.

                        ingest_file(iqe)

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    # ingest_file() should handle its own exceptions, and
                    # should never raise, so if there's a problem with a given
                    # file we log the error and continue with the next one.
                    # This catches anything else in the code above. Again, we
                    # log the error and carry on. Probably the error would
                    # reoccur if we re-try the same file though, so we set it
                    # as failed and record the error in the iqe too.
                    message = "Unknown Error - no IngestQueueEntry instance"
                    if iqe:
                        iqe.failed = True
                        tbs = "".join(traceback.format_tb(sys.exc_info()[2]))
                        message = f"Exception: {sys.exc_info()[0]} : " \
                                  f"{sys.exc_info()[1]} ... {tbs}"
                        iqe.error = message
                        session.commit()
                    logger.error(message)

                    # Press on with the next file, don't raise the exception

    except PidFileError as e:
        logger.error(str(e))

    logger.info("***    service_ingest_queue.py - exiting at "
                f"{datetime.datetime.now()}")
