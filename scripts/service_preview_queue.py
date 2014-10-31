#! /usr/bin/env python
from orm import sessionfactory
from orm.previewqueue import PreviewQueue
from orm.diskfile import DiskFile

from fits_storage_config import fits_lockfile_dir
from utils.previewqueue import pop_previewqueue, previewqueue_length, make_preview
from logger import logger, setdebug, setdemon, setlogfilesuffix
import signal
import sys
import os
import datetime
import time
import traceback

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--name", action="store", dest="name", help="Name for this process. Used in logfile and lockfile")
parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")
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
logger.info("*********    service_preview_queue.py - starting up at %s", datetime.datetime.now())

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

session = sessionfactory()

# Loop forever. loop is a global variable defined up top
while loop:
    try:
        # Request a queue entry
        pq = pop_previewqueue(session)

        if pq is None:
            logger.info("Nothing on queue.")
            if options.empty:
                logger.info("--empty flag set, exiting")
                break
            else:
                logger.info("...Waiting")
            time.sleep(10)
        else:
            logger.info("Making preview for %d, (%d in queue)", pq.diskfile_id, previewqueue_length(session))

            try:
                # Actually make the preview here
                # Get the diskfile
                diskfile = session.query(DiskFile).filter(DiskFile.id == pq.diskfile_id).one()
                # make the preview
                make_preview(session, diskfile)
            except:
                logger.info("Problem Making Preview - Rolling back")
                logger.error("Exception making preview %s: %s : %s... %s", pq.diskfile_id, sys.exc_info()[0], sys.exc_info()[1], traceback.format_tb(sys.exc_info()[2]))
                session.rollback()
                # We leave inprogress as True here, because if we set it back to False, we get immediate retry and rapid failures
                # pq.inprogress=False
                raise
            logger.debug("Deleteing previewqueue id %d", pq.id)
            # pq is a transient ORM object, find it in the db
            dbpq = session.query(PreviewQueue).filter(PreviewQueue.id == pq.id).one()
            session.delete(dbpq)
            session.commit()

    except KeyboardInterrupt:
        loop = False

    except:
        string = traceback.format_tb(sys.exc_info()[2])
        string = "".join(string)
        session.rollback()
        logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1], string)
        # Press on with the next file, don't raise the esception further.
        # raise

    finally:
        session.close()

if options.lockfile:
    logger.info("Deleting Lockfile %s", lockfile)
    os.unlink(lockfile)
logger.info("*********    service_preview_queue.py - exiting at %s", datetime.datetime.now())
