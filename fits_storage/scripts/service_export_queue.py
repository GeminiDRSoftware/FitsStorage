#! /usr/bin/env python3

import signal
import datetime
import time

from argparse import ArgumentParser

from fits_storage.queues.queue.exportqueue import ExportQueue
from fits_storage.server.exporter import Exporter
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError
from fits_storage.db import session_scope

from fits_storage.config import get_config

fsc = get_config()

parser = ArgumentParser(prog='service_export_queue.py',
                        description='service the FitsStorage export queue')
parser.add_argument("--debug", action="store_true", dest="debug", default=False,
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon", default=False,
                    help="Run as a background demon, do not generate stdout")
parser.add_argument("--name", action="store", dest="name",
                    help="Name for this process. Used in logfile and lockfile")
parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                    help="Use a lockfile to limit instances")
parser.add_argument("--empty", action="store_true", default=False, dest="empty",
                    help="Exit once the queue is empty.")
parser.add_argument("--oneshot", action="store_true", dest="oneshot",
                    default=False, help="Process only one file then exit")
parser.add_argument("--fast-rebuild", action="store_true", dest="fastrebuild",
                    help="Do not report queue length for faster processing of "
                         "very long queues")
parser.add_argument("--retry-delay", action="store", type=int, dest="delay",
                    default=60, help="Number of seconds to delay retries after "
                                     "the queue becomes empty")
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
    logger.error("Received signal: %d. Crashing out. ", signum)
    raise KeyboardInterrupt('Signal', signum)


def nicehandler(signum, frame):
    logger.error("Received signal: %d. Attempting to stop nicely.", signum)
    global loop
    loop = False


# Set handlers for the signals we want to handle
# Cannot trap SIGKILL or SIGSTOP, all others are fair game
# Don't trap SIGPIPE - if that happens, we want to see the exception.
signal.signal(signal.SIGHUP, nicehandler)
signal.signal(signal.SIGINT, nicehandler)
signal.signal(signal.SIGQUIT, nicehandler)
signal.signal(signal.SIGILL, handler)
signal.signal(signal.SIGABRT, handler)
signal.signal(signal.SIGFPE, handler)
signal.signal(signal.SIGSEGV, handler)
signal.signal(signal.SIGTERM, nicehandler)

# Announce startup
logger.info("***   service_export_queue.py - starting up at %s",
            datetime.datetime.now())
logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

try:
    with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, \
            session_scope() as session:

        export_queue = ExportQueue(session, logger=logger)
        exporter = Exporter(session, logger, timeout=10)

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
                        export_queue.retry_failures(options.delay)
                        continue

                if options.oneshot:
                    loop = False

                # Don't query queue length in fast_rebuild mode
                if options.fastrebuild:
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

            except KeyboardInterrupt:
                logger.error("KeyboardInterrupt - exiting ungracefully!")
                loop = False
                break

            except:
                # export_file() should handle its own exceptions, and
                # should never raise, so if there's a problem with a given
                # file we log the error and continue with the next one.
                # This catches anything else in the code above. Again, we
                # log the error and carry on. Probably the error would
                # reoccur if we re-try the same file though, so we set it
                # as failed and record the error in the eqe too.
                logger.error("Unhandled Exception in service_export_queue!",
                             exc_info=True)
                message = "Unknown Error - no ExportQueueEntry instance"
                if eqe is not None:
                    eqe.failed = True
                    eqe.inprogress = False
                    message = "Exception in service_export_queue while " \
                              f"processing {eqe.filename}"
                    eqe.error = message
                    session.commit()

                logger.error(message)
                # This is drastic. raise the exception so we crash out.
                # We need to figure out what causes any occurence off this.
                raise

except PidFileError as e:
    logger.error(str(e))

logger.info("***   service_export_queue.py - exiting at %s",
            datetime.datetime.now())
