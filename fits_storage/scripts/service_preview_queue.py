#! /usr/bin/env python3

import signal
import sys
import datetime
import time
import traceback
from sqlalchemy.exc import OperationalError
from argparse import ArgumentParser

from fits_storage.config import get_config

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.db import session_scope
from fits_storage.queues.queue.previewqueue import PreviewQueue

from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.server.previewer import Previewer

parser = ArgumentParser()
parser.add_argument("--debug", action="store_true", dest="debug", default=False,
                    help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon", default=False,
                    help="Run as a background demon, do not generate stdout")
parser.add_argument("--name", action="store", dest="name",
                    help="Name for this process. Used in logfile and lockfile")
parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                    help="Use a lockfile to limit instances")
parser.add_argument("--empty", action="store_true", default=False, dest="empty",
                    help="Exit when the queue is empty")
parser.add_argument("--oneshot", action="store_true", default=False,
                    dest="oneshot", help="Process one queue entry then exit")
parser.add_argument("--fast-rebuild", action="store_true", dest="fast_rebuild",
                    help="Fast rebuild mode")
options = parser.parse_args()

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
fsc = get_config()
logger.info("***   service_preview_queue.py - starting up at %s",
            datetime.datetime.now())
logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

try:
    with PidFile(logger, name=options.name, dummy=not options.lockfile) as pidfile, \
            session_scope() as session:
        pq = PreviewQueue(session, logger=logger)
        # Loop forever. loop is a global variable defined up top
        while loop:
            try:
                # Request a queue entry
                pqe = pq.pop()

                if pqe is None:
                    if options.empty:
                        logger.info("--empty flag set, exiting")
                        break
                    else:
                        logger.info("Nothing on queue... Waiting")
                        time.sleep(5)
                        continue

                if options.oneshot:
                    loop = False

                # Don't query queue length in fast_rebuild mode
                if options.fast_rebuild:
                    logger.info("Previewing %s - %s", pqe.filename, pqe.id)
                else:
                    logger.info("Previewing %s - %s (%d on queue)",
                                pqe.filename, pqe.id, pq.length())

                # Actually do the preview here
                try:
                    # Get the diskfile object
                    df = session.get(DiskFile, pqe.diskfile_id)

                    # Make the previewer instance
                    p = Previewer(df, session, logger=logger, force=pqe.force,
                                  scavengeonly=pqe.scavengeonly)

                    if p.make_preview():
                        # Success
                        logger.debug("make_preview() succeeded for %s",
                                     pqe.filename)
                        session.delete(pqe)
                    else:
                        # Failed
                        logger.error("Failed to make preview for %s",
                                     pqe.filename)
                        pqe.inprogress = False
                        pqe.failed = True
                        pqe.seterror("Bad Status from make_preview()")
                        session.commit()

                except (KeyboardInterrupt, OperationalError):
                    loop = False

                except:
                    logger.info("Problem Making Preview")
                    logger.error("Exception making preview for %s",
                                 pqe.filename, exc_info=True)
                    pqe.inprogress = False
                    pqe.failed = True
                    pqe.seterror("Exception making preview")
                    session.commit()
            except:
                logger.error("Exception in service_preview_queue", exc_info=True)
except PidFileError as e:
    logger.error(str(e))

logger.info("***   service_preview_queue.py - exiting at %s",
            datetime.datetime.now())
