#! /usr/bin/env python3

import signal
import sys
import datetime
import time
import traceback
import requests
import ssl
from requests import RequestException

from sqlalchemy.exc import OperationalError, IntegrityError
from optparse import OptionParser

from fits_storage.queues.queue.exportqueue import ExportQueue
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError
from fits_storage.db import session_scope


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug",
                      default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      default=False,
                      help="Run as a background demon, do not generate stdout")
    parser.add_option("--name", action="store", dest="name",
                      help="Name for this process. "
                           "Used in logfile and lockfile")
    parser.add_option("--lockfile", action="store_true", dest="lockfile",
                      help="Use a lockfile to limit instances")
    parser.add_option("--empty", action="store_true", default=False,
                      dest="empty", help="Exit once the queue is empty.")
    parser.add_argument("--oneshot", action="store_true", dest="oneshot",
                        default=False, help="Process only one file then exit")
    (options, args) = parser.parse_args()

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
        logger.error("Received signal: %d. Crashing out. ", signum)
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

    # Annouce startup
    logger.info("***   service_export_queue.py - starting up at %s",
                datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    try:
        with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, \
                session_scope() as session:

            export_queue = ExportQueue(session, logger=logger)

            # Loop forever. loop is a global variable defined up top
            while loop:
                try:
                    # Request a queue entry. The returned entry is marked
                    # as inprogress and committed to the session.
                    eqe = export_queue.pop()

                    if eqe is None:
                        if options.empty:
                            logger.info("Nothing on queue and "
                                        "--empty flag set, exiting")
                            break
                        else:
                            logger.info("Nothing on Queue... Waiting")
                            time.sleep(2)
                            # Mark any old failures for retry
                            export_queue.retry_failures(interval)
                            continue

                    if options.oneshot:
                        loop = False

                    # Don't query queue length in fast_rebuild mode
                    if options.fast_rebuild:
                        logger.info("Exporting %s - %s" %
                                    (eqe.filename, eqe.id))
                    else:
                        logger.info("Exporting %s - %d (%d on queue)" %
                                    (eqe.filename, eqe.id,
                                     export_queue.length()))

                    # Go ahead and export the file. At this point, eqe is
                    # marked as inprogress and is committed to the database.
                    # export_file(eqe) should handle everything from here -
                    # including deleting the eqe if it successfully exports
                    # it, setting the status and error messages in eqe and
                    # outputting appropriate log messages if there's a failure.

                    exporter.export_file(eqe)

                    try:
                        success, details = export_queue.export_file(eq.filename, eq.path, eq.destination,
                                                               header_fields=eq.header_fields,
                                                               md5_before_header=eq.md5_before_header,
                                                               md5_after_header=eq.md5_after_header,
                                                               reject_new=eq.reject_new)
                        except (RequestException, ssl.SSLError, ValueError):
                            logger.info("Problem Exporting File - Rolling back")
                            # Originally we set the inprogress flag back to False at the point that we abort.
                            # But that can lead to an immediate re-try and subsequent rapid rate re-failures,
                            # and it will never move on to the next file. So leave it set inprogress to avoid that.

                            # Setting the error may be pointless, because the export will be tried again in
                            # (maybe) a few minutes, but let's do it for consistency
                            export_queue.set_error(eq, *sys.exc_info())
                            raise
                        if success:
                            logger.debug("Deleting exportqueue id %d", eq.id)
                            export_queue.delete(eq)
                        else:
                            if details == "pending ingest":
                                # we just have to defer it
                                export_queue.set_deferred(eq)
                            else:
                                logger.info(f"Exportqueue id %d DID NOT TRANSFER", eq.id)
                                # The eq instance we have is transient - get one connected to the session
                                export_queue.set_last_failed(eq)
                        session.commit()

                    throttle.reset()

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except IntegrityError as e:
                    logger.error("Nothing on export queue - Exception: %s", str(e))
                    throttle.wait()
                    session.rollback()
                except:
                    string = traceback.format_tb(sys.exc_info()[2])
                    string = "".join(string)
                    if eq:
                        logger.error("File %s - Exception: %s : %s... %s", eq.filename, sys.exc_info()[0], sys.exc_info()[1], string)
                    else:
                        logger.error("Nothing on export queue - Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
                    # Make sure that the session ends up in a consistent state
                    session.rollback()
                    # Prevent fast fail loop
                    time.sleep(5)

    except PidFileError as e:
        logger.error(str(e))

    logger.info("*********    service_export_queue.py - exiting at %s", datetime.datetime.now())
