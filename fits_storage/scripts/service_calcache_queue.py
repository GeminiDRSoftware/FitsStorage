#! /usr/bin/env python3

import signal
import datetime
import time

from argparse import ArgumentParser

from fits_storage.db import session_scope
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.queues.queue import CalCacheQueue


from fits_storage.config import get_config
fsc = get_config()

parser = ArgumentParser(prog='service_calcache_queue.py',
                        description='Service the FitsStorage CalCache Queue')
parser.add_argument("--fast-rebuild", action="store_true", dest="fast_rebuild",
                    default=False,
                    help="Fast rebuild mode - skip duplication checking etc")
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
parser.add_argument("--oneshot", action="store_true", default=False,
                    dest="oneshot", help="Process one queue entry then exit")
options = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

if options.name:
    setlogfilesuffix(options.name)

# Need to set up the global loop variable before we define the signal
# handlers.
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
    with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, \
            session_scope() as session:

        ccq = CalCacheQueue(session, logger)

        # Loop forever. loop is a global variable defined up top
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
                                (ccqe.obs_hid, ccqe.filename))
                else:
                    logger.info("Processing obs_hid %d - %s, (%d in queue)"
                                % (ccqe.obs_hid, ccqe.filename,
                                   ccq.length()))

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

            except KeyboardInterrupt:
                logger.error("KeyboardInterrupt - exiting ungracefully!")
                loop = False
                break

            except:
                logger.error("Unhandled Exception in service_calcache_queue!",
                             exc_info=True)
                raise

except PidFileError as e:
    logger.error(str(e))

logger.info("***    service_calcache_queue.py - exiting at %s",
            datetime.datetime.now())
