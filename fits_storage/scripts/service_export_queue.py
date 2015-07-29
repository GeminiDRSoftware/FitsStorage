#! /usr/bin/env python
import signal
import sys
import os
import datetime
import time
import traceback
from fits_storage.orm import sessionfactory
from fits_storage.orm.exportqueue import ExportQueue
from fits_storage.utils.exportqueue import export_file, pop_exportqueue, exportqueue_length, retry_failures
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.fits_storage_config import fits_lockfile_dir

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--retry_mins", action="store", dest="retry_mins", type="float", default=5.0, help="Minimum number of minutes to wait before retries")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--name", action="store", dest="name", help="Name for this process. Used in logfile and lockfile")
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

if options.lockfile:
    # Does the Lockfile exist?
    lockfile = "%s/%s.lock" % (fits_lockfile_dir, options.name)
    if os.path.exists(lockfile):
        logger.info("Lockfile %s already exists, testing for viability", lockfile)
        actually_locked = True
        try:
            # Read the pid from the lockfile
            lfd = open(lockfile, 'r')
            oldpid = int(lfd.read())
            lfd.close()
        except:
            logger.error("Could not read pid from lockfile %s", lockfile)
            oldpid = 0
        # Try and send a null signal to test if the process is viable.
        try:
            if oldpid:
                os.kill(oldpid, 0)
        except:
            # If this gets called then the lockfile refers to a process which either doesn't exist or is not ours.
            logger.error("PID in lockfile prefers to a process which either doesn't exist, or is not ours - %d", oldpid)
            actually_locked = False

        if actually_locked:
            logger.info("Lockfile %s refers to PID %d which appears to be valid. Exiting", lockfile, oldpid)
            sys.exit()
        else:
            logger.error("Lockfile %s refers to PID %d which appears to be not us. Deleting lockfile", lockfile, oldpid)
            os.unlink(lockfile)
            logger.info("Creating replacement lockfile %s", lockfile)
            lfd = open(lockfile, 'w')
            lfd.write("%s\n" % os.getpid())
            lfd.close()

    else:
        logger.info("Lockfile does not exist: %s", lockfile)
        logger.info("Creating new lockfile %s", lockfile)
        lfd = open(lockfile, 'w')
        lfd.write("%s\n" % os.getpid())
        lfd.close()

# retry interval option has a default so should always be defined
interval = datetime.timedelta(minutes=options.retry_mins)

session = sessionfactory()

# Loop forever. loop is a global variable defined up top
while loop:
    try:
        # Request a queue entry
        logger.debug("Requesting an exportqueue entry")
        eq = pop_exportqueue(session, logger)

        if eq is None:
            if options.empty:
                logger.info("Nothing on queue and --empty flag set, exiting")
                break
            else:
                logger.info("Nothing on Queue... Waiting")
            time.sleep(2)

            # Mark any old failures for retry
            retry_failures(session, logger, interval)

        else:
            logger.info("Exporting %s, (%d in queue)", eq.filename, exportqueue_length(session))

            try:
                sucess = export_file(session, logger, eq.filename, eq.path, eq.destination)
            except:
                logger.info("Problem Exporting File - Rolling back")
                session.rollback()
                # Originally we set the inprogress flag back to False at the point that we abort.
                # But that can lead to an immediate re-try and subsequent rapid rate re-failures,
                # and it will never move on to the next file. So leave it set inprogress to avoid that.
                raise
            if sucess:
                logger.debug("Deleteing exportqueue id %d", eq.id)
                session.query(ExportQueue).filter(ExportQueue.id == eq.id).delete()
                session.commit()
            else:
                logger.info("Exportqueue id %d DID NOT TRANSFER", eq.id)
                # The eq instance we have is transient - get one connected to the session
                dbeq = session.query(ExportQueue).filter(ExportQueue.id == eq.id).one()
                dbeq.lastfailed = datetime.datetime.now()
                session.commit()

    except KeyboardInterrupt:
        loop = False

    except:
        string = traceback.format_tb(sys.exc_info()[2])
        string = "".join(string)
        if eq:
            logger.error("File %s - Exception: %s : %s... %s", eq.filename, sys.exc_info()[0], sys.exc_info()[1], string)
        else:
            logger.error("Nothing on export queue - Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
    finally:
        session.close()

session.close()
if options.lockfile:
    logger.info("Deleting Lockfile %s", lockfile)
    os.unlink(lockfile)
logger.info("*********    service_export_queue.py - exiting at %s", datetime.datetime.now())
