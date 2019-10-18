#! /usr/bin/env python
import signal
import sys
import os
import datetime
import time
import traceback
import urllib.request, urllib.error, urllib.parse
import ssl
from fits_storage.orm import session_scope
from fits_storage.orm.exportqueue import ExportQueue
from fits_storage.utils.exportqueue import ExportQueueUtil
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.fits_storage_config import fits_lockfile_dir
from fits_storage.utils.pidfile import PidFile, PidFileError
from sqlalchemy.exc import OperationalError, IntegrityError


from optparse import OptionParser

parser = OptionParser()
parser.add_option("--retry_mins", action="store", dest="retry_mins", type="float", default=5.0, help="Minimum number of minutes to wait before retries")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--name", action="store", default="service_export_queue", dest="name", help="Name for this process. Used in logfile and lockfile")
parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")
parser.add_option("--empty", action="store_true", default=False, dest="empty", help="This flag indicates that service ingest queue should empty the current queue and then exit.")
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

class Throttle(object):
    def __init__(self, cap = 60):
        self.cap = cap
        self.reset()

    def reset(self):
        self.t1 = 1
        self.t2 = 1

    def wait(self):
        slp = min(self.t1 + self.t2, self.cap)
        logger.info("* Throttling: will sleep for %d seconds" % slp)
        time.sleep(slp)
        if slp < self.cap:
            self.t1, self.t2 = self.t2, self.t1 + self.t2

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
logger.info("*********    service_export_queue.py - starting up at %s", datetime.datetime.now())

throttle = Throttle()

try:
    with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile, session_scope() as session:
        # retry interval option has a default so should always be defined
        interval = datetime.timedelta(minutes=options.retry_mins)
        eq = None

        export_queue = ExportQueueUtil(session, logger)
        # Loop forever. loop is a global variable defined up top
        while loop:
            try:
                # Request a queue entry
                logger.debug("Requesting an exportqueue entry")
                eq = export_queue.pop()

                if eq is None:
                    if options.empty:
                        logger.info("Nothing on queue and --empty flag set, exiting")
                        break
                    else:
                        logger.info("Nothing on Queue... Waiting")
                    time.sleep(2)

                    # Mark any old failures for retry
                    export_queue.retry_failures(interval)

                else:
                    logger.info("Exporting %s, (%d in queue)", eq.filename, export_queue.length())

                    try:
                        success = export_queue.export_file(eq.filename, eq.path, eq.destination)
                    except (urllib.error.URLError, ssl.SSLError, ValueError):
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
                        logger.info("Exportqueue id %d DID NOT TRANSFER", eq.id)
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
