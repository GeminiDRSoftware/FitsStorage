#! /usr/bin/env python3

import datetime
import signal
import time

from sqlalchemy.exc import OperationalError, IntegrityError

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.db import sessionfactory
from fits_storage.queues.queue import IngestQueue

from fits_storage.core.ingester import Ingester

from fits_storage.config import get_config
fsc = get_config()


parser = ArgumentParser(prog='service_ingest_queue.py',
                        description='Service the FitsStorage Ingest Queue')
parser.add_argument("--skip-fv", action="store_true", dest="skip_fv",
                    default=False, help="Do not fitsverify the files")
parser.add_argument("--skip-md", action="store_true", dest="skip_md",
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
parser.add_argument("--demon", action="store_true", dest="demon", default=False,
                    help="Run as background demon, do not generate stdout")
parser.add_argument("--name", action="store", dest="name",
                    help="Name for this instance of this task. "
                         "Used in logfile and lockfile")
parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                    help="Use a lockfile to limit instances")
parser.add_argument("--empty", action="store_true", dest="empty", default=False,
                    help="Exit once the queue is empty.")
parser.add_argument("--oneshot", action="store_true", dest="oneshot",
                    default=False, help="Process only one file then exit")
options = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

if options.name is not None:
    setlogfilesuffix(options.name)

# Need to set up the global loop variable before we define the signal handlers
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
signal.signal(signal.SIGTERM, handler)

# Announce startup
logger.info("***   service_ingest_queue.py - starting up at %s",
            datetime.datetime.now())
logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

try:
    with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile:
        session = sessionfactory()

        ingest_queue = IngestQueue(session, logger)
        ingester = Ingester(session, logger, skip_fv=options.skip_fv,
                            skip_md=options.skip_md)

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
                        continue

                if options.oneshot:
                    loop = False

                # Don't query queue length in fast_rebuild mode
                if options.fast_rebuild:
                    logger.info("Ingesting %s - %s" %
                                (iqe.filename, iqe.id))
                else:
                    logger.info("Ingesting %s - %s (%d on queue)" %
                                (iqe.filename, iqe.id,
                                 ingest_queue.length()))

                # Check if the file was very recently modified or is
                # locked, defer ingestion if so
                if not (fsc.using_s3 or options.no_defer):
                    defer_message = iqe.defer()
                    if defer_message is not None:
                        logger.info(defer_message)
                        try:
                            iqe.inprogress = False
                            session.commit()
                        except (IntegrityError, OperationalError):
                            # Possible race condition here if same file
                            # has been added to queue with inprogress=False
                            # while we were doing this, preventing us
                            # setting this one back to false. Ideally we
                            # just delete the current entry at this point.
                            logger.error("Possible Deferred file race "
                                         "condition in service_ingest_"
                                         "queue. Update code to handle "
                                         "this", exc_info=True)
                        except Exception:
                            logger.error("This should not happen - Need "
                                         "to handle this exception in "
                                         "service_ingest_queue!",
                                         exc_info=True)
                        continue

                # Go ahead and ingest the file. At this point, iqe is
                # marked as inprogress and is committed to the database.
                # ingest_file(iqe) should handle everything from here -
                # including deleting the iqe if it successfully ingests
                # it, adding it to the export queue if appropriate and
                # setting the status and error messages in iqe and
                # outputting appropriate log messages if there's a failure.

                ingester.ingest_file(iqe)

            except KeyboardInterrupt:
                logger.error("KeyboardInterrupt - exiting ungracefully!")
                loop = False
                break

            except:
                # ingest_file() should handle its own exceptions, and
                # should never raise, so if there's a problem with a given
                # file we log the error and continue with the next one.
                # This catches anything else in the code above. Again, we
                # log the error and carry on. Probably the error would
                # reoccur if we re-try the same file though, so we set it
                # as failed and record the error in the iqe too.

                # Log the exception right away so that if the handling fails
                # we still get an error message
                logger.error("Unhandled Exception in service_ingest_queue!",
                             exc_info=True)
                message = "Unknown Error - no IngestQueueEntry instance"
                if iqe is not None:
                    try:
                        iqe.failed = True
                        iqe.inprogress = False
                        message = "Exception in service_ingest_queue " \
                                  "while processing {iqe.filename}"
                        iqe.error = message
                        session.commit()
                    except:
                        logger.error("Exception while trying to handle "
                                     "exception in service_ingest_queue",
                                     exc_info=True)

                logger.error(message)
                # This is drastic. raise the exception so we crash out.
                # We need to figure out what causes any occurence off this.
                raise
except PidFileError as e:
    logger.error(str(e))

logger.info("***    service_ingest_queue.py - exiting at %s",
            datetime.datetime.now())
