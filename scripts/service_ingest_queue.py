#! /usr/bin/env python
from orm import sessionfactory
from fits_storage_config import using_sqlite, using_s3, storage_root, defer_seconds, fits_lockfile_dir, export_destinations
from utils.ingestqueue import ingest_file, pop_ingestqueue, ingestqueue_length
from utils.exportqueue import add_to_exportqueue
from logger import logger, setdebug, setdemon, setlogfilesuffix
import signal
import sys
import os
import datetime
import time
import traceback

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--skip-fv", action="store_true", dest="skip_fv", default=False, help="Do not run fitsverify on the files")
parser.add_option("--skip-wmd", action="store_true", dest="skip_wmd", default=False, help="Do not run a wmd check on the files")
parser.add_option("--no-defer", action="store_true", dest="no_defer", default=False, help="Do not defer ingestion of recently modified files")
parser.add_option("--fast-rebuild", action="store_true", dest="fast_rebuild", default=False, help="Fast rebuild mode - skip duplication checking etc")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--name", action="store", dest="name", help="Name for this process. Used in logfile and lockfile")
parser.add_option("--lockfile", action="store_true", dest="lockfile", help="Use a lockfile to limit instances")
parser.add_option("--empty", action="store_true", default=False, dest="empty", help="This flag indicates that service ingest queue should empty the current queue and then exit.")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)
if(options.name):
    setlogfilesuffix(options.name)

# Need to set up the global loop variable before we define the signal handlers
# This is the loop forever variable later, allowing us to stop cleanly via kill
global loop
loop = True

# Define signal handlers. This allows us to bail out neatly if we get a signal
def handler(signum, frame):
    logger.error("Received signal: %d. Crashing out. " % signum)
    raise KeyboardInterrupt('Signal', signum)

def nicehandler(signum, frame):
    logger.error("Received signal: %d. Attempting to stop nicely " % signum)
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
now = datetime.datetime.now()
logger.info("*********    service_ingest_queue.py - starting up at %s" % now)

if(options.lockfile):
    # Does the Lockfile exist?
    lockfile = "%s/%s.lock" % (fits_lockfile_dir, options.name)
    if(os.path.exists(lockfile)):
        logger.info("Lockfile %s already exists, testing for viability" % lockfile)
        actually_locked = True
        try:
            # Read the pid from the lockfile
            lfd = open(lockfile, 'r')
            oldpid = int(lfd.read())
            lfd.close()
        except:
            logger.error("Could not read pid from lockfile %s" % lockfile)
            oldpid = 0
        # Try and send a null signal to test if the process is viable.
        try:
            if(oldpid):
                os.kill(oldpid, 0)
        except:
            # If this gets called then the lockfile refers to a process which either doesn't exist or is not ours.
            logger.error("PID in lockfile prefers to a process which either doesn't exist, or is not ours - %d" % oldpid)
            actually_locked = False

        if(actually_locked):
            logger.info("Lockfile %s refers to PID %d which appears to be valid. Exiting" % (lockfile, oldpid))
            sys.exit()
        else:
            logger.error("Lockfile %s refers to PID %d which appears to be not us. Deleting lockfile" % (lockfile, oldpid))
            os.unlink(lockfile)
            logger.info("Creating replacement lockfile %s" % lockfile)
            lfd = open(lockfile, 'w')
            lfd.write("%s\n" % os.getpid())
            lfd.close()

    else:
        logger.info("Lockfile does not exist: %s" % lockfile)
        logger.info("Creating new lockfile %s" % lockfile)
        lfd = open(lockfile, 'w')
        lfd.write("%s\n" % os.getpid())
        lfd.close()

session = sessionfactory()

# Loop forever. loop is a global variable defined up top
while(loop):
    try:
        # Request a queue entry
        iq = pop_ingestqueue(session, options.fast_rebuild)

        if(iq==None):
            logger.info("Didn't get anything to ingest, retrying")
            iq = pop_ingestqueue(session)
        if(iq==None):
            logger.info("Nothing on queue.")
            if options.empty:
                logger.info("--empty flag set, exiting")
                break
            else:
                logger.info("...Waiting")
            time.sleep(10)
        else:
            # Don't query queue length in fast_rebuild mode
            if(options.fast_rebuild):
                logger.info("Ingesting %s" % iq.filename)
            else:
                logger.info("Ingesting %s, (%d in queue)" % (iq.filename, ingestqueue_length(session)))

            if(using_sqlite):
                # SQLite doesn't support nested transactions
                session.begin(subtransactions=True)
            else:
                session.begin_nested()

            # Check if the file was very recently modified, defer ingestion if it was
            if((not using_s3) and (options.no_defer == False) and (defer_seconds > 0)):
                fullpath = os.path.join(storage_root, iq.path, iq.filename)
                lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                now = datetime.datetime.now()
                age = now - lastmod
                defer = datetime.timedelta(seconds=defer_seconds)
                if(age < defer):
                    logger.info("Deferring ingestion of file %s" % iq.filename)
                    # Defer ingestion of this file for defer_secs
                    after = now + defer
                    iq.after = after
                    iq.inprogress = False
                    # Need two commits here, one for each layer of the nested transaction
                    session.commit()
                    session.commit()
                    continue
            try:
                added_diskfile = ingest_file(session, iq.filename, iq.path, iq.force_md5, iq.force, options.skip_fv, options.skip_wmd)
                session.commit()
                # Now we also add this file to our export list if we have downstream servers and we did add a diskfile
                if added_diskfile:
                    for destination in export_destinations:
                        add_to_exportqueue(session, iq.filename, iq.path, destination)
            except:
                logger.info("Problem Ingesting File - Rolling back" )
                logger.error("Exception ingesting file %s: %s : %s... %s" % (iq.filename, sys.exc_info()[0], sys.exc_info()[1], traceback.format_tb(sys.exc_info()[2])))
                session.rollback()
                # We leave inprogress as True here, because if we set it back to False, we get immediate retry and rapid failures
                # iq.inprogress=False
                session.commit()
                raise
            logger.debug("Deleteing ingestqueue id %d" % iq.id)
            session.delete(iq)
            session.commit()

    except KeyboardInterrupt:
        loop = False

    except:
        string = traceback.format_tb(sys.exc_info()[2])
        string = "".join(string)
        if(iq):
            logger.error("File %s - Exception: %s : %s... %s" % (iq.filename, sys.exc_info()[0], sys.exc_info()[1], string))
        else:
            logger.error("Nothing on ingest queue - Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

    finally:
        session.close()

session.close()
if(options.lockfile):
    logger.info("Deleting Lockfile %s" % lockfile)
    os.unlink(lockfile)
logger.info("*********    service_ingest_queue.py - exiting at %s" % datetime.datetime.now())

