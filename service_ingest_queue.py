#! /usr/bin/env python
import FitsStorage
import FitsStorageConfig
from FitsStorageUtils.ServiceIngestQueue import *
from FitsStorageLogger import *
import signal
import os
import re
import datetime
import time
import traceback

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--force-crc", action="store_true", dest="force_crc", default=False, help="Force crc check on pre-existing files")
parser.add_option("--force", action="store_true", dest="force", default=False, help="Force re-ingest of file regardless")
parser.add_option("--skip-fv", action="store_true", dest="skip_fv", default=False, help="Do not run fitsverify on the files")
parser.add_option("--skip-wmd", action="store_true", dest="skip_wmd", default=False, help="Do not run a wmd check on the files")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--lockfile", action="store", dest="lockfile", help="Use this as a lockfile to limit instances")
parser.add_option("--empty", action="store_true", default=False, dest="empty", help="This flag indicates that service ingest queue should empty the current queue and then exit.")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Need to set up the global loop variable before we define the signal handlers
# This is the loop forever variable later, allowing us to stop cleanly via kill
global loop
loop=True

# Define signal handlers. This allows us to bail out neatly if we get a signal
def handler(signum, frame):
  logger.error("Received signal: %d. Crashing out. " % signum)
  raise KeyboardInterrupt('Signal', signum)

def nicehandler(signum, frame):
  logger.error("Received signal: %d. Attempting to stop nicely " % signum)
  global loop
  loop=False

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
logger.info("*********  service_ingest_queue.py - starting up at %s" % now)

if(options.lockfile):
  # Does the Lockfile exist?
  lockfile = "%s/%s" % (FitsStorageConfig.fits_lockfile_dir, options.lockfile)
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
      oldpid=0
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
      lfd=open(lockfile, 'w')
      lfd.write("%s\n" % os.getpid())
      lfd.close()

  else:
    logger.info("Lockfile does not exist: %s" % lockfile)
    logger.info("Creating new lockfile %s" % lockfile)
    lfd=open(lockfile, 'w')
    lfd.write("%s\n" % os.getpid())
    lfd.close()

session = sessionfactory()

# Loop forever. loop is a global variable defined up top
while(loop):
  try:
    # Request a queue entry
    iq = pop_ingestqueue(session)

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
      logger.info("Ingesting %s, (%d in queue)" % (iq.filename, ingestqueue_length(session)))
      if(FitsStorageConfig.using_sqlite):
        # SQLite doesn't support nested transactions
        session.begin(subtransactions=True)
      else:
        session.begin_nested()

      try:
        ingest_file(session, iq.filename, iq.path, options.force_crc, options.force, options.skip_fv, options.skip_wmd)
        session.commit()
      except:
        logger.info("Problem Ingesting File - Rolling back" )
        session.rollback()
        # Originally we set the inprogress flag back to False at the point that we abort. But that can lead to an immediate re-try
        # and subsequent rapid rate re-failures, and it will never move on to the next file. So lets try leaving it set inprogress to avoid that.
        # iq.inprogress=False
        session.commit()
        raise
      logger.debug("Deleteing ingestqueue id %d" % iq.id)
      session.delete(iq)
      session.commit()

  except KeyboardInterrupt:
    loop=False

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
logger.info("*********  service_ingest_queue.py - exiting at %s" % datetime.datetime.now())

