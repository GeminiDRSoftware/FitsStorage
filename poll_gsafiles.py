#! /usr/bin/env python
from FitsStorage import *
import FitsStorageConfig
from FitsStorageLogger import *
import signal
import os
import re
import datetime
import time
import traceback
import CadcCRC

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--lockfile", action="store", dest="lockfile", help="Use this as a lockfile to limit instances")
parser.add_option("--authfile", action="store", dest="authfile", default="/home/fitsdata/.gsaauth", help="File containing the authentication credentials to the GSA")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Try and get the GSA authentication details
try:
  af = open(options.authfile, 'r')
  gsa_user = af.readline().strip()
  gsa_pass = af.readline().strip()
except:
  logger.error("Failed to read GSA authentication details. Aborting")
  sys.exit()


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
logger.info("*********  poll_gsafiles.py - starting up at %s" % now)

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
      logger.info("Could not read pid from lockfile %s" % lockfile)
      oldpid=0
    # Try and send a null signal to test if the process is viable.
    try:
      if(oldpid):
        os.kill(oldpid, 0)
    except:
      # If this gets called then the lockfile refers to a process which either doesn't exist or is not ours.
      actually_locked = False

    if(actually_locked):
      logger.info("Lockfile %s refers to PID %d which appears to be valid. Exiting" % (lockfile, oldpid))
      sys.exit()
    else:
      logger.info("Lockfile %s refers to PID %d which appears to be not us. Deleting lockfile" % (lockfile, oldpid))
      os.unlink(lockfile)
  else:
    logger.info("Creating lockfile %s" % lockfile)
    lfd=open(lockfile, 'w')
    lfd.write("%s\n" % os.getpid())
    lfd.close()

session = sessionfactory()

# Loop forever. loop is a global variable defined up top
while(loop):
  try:
    file_id = None

    if(file_id is None):
      # First priority is to (re-) poll files where the canonical diskfile lastmod is more recent than the gsafile poll date
      query = session.query(File).select_from(DiskFile, File, GsaFile)
      query = query.filter(DiskFile.file_id == File.id).filter(GsaFile.file_id == File.id).filter(DiskFile.canonical == True)
      query = query.filter(DiskFile.lastmod > GsaFile.lastpoll)
      query = query.order_by(desc(DiskFile.lastmod)).limit(1)
      num = query.count()
      if(num > 0):
        f = query.first()
        file_id = f.id
        logger.info("Found file %d %s which has been modified since last polled" % (f.id, f.name))
        gquery = session.query(GsaFile).filter(GsaFile.file_id == file_id)
        gf = query.one()
        gsainfo = CadcCRC.get_gsa_info(f.filename, gsa_user, gsa_pass)
        gf.md5sum = gsainfo['md5sum']
        gf.ingestdate = gsainfo['ingestdate']
        gf.lastpoll = datetime.datetime.now()
        session.commit()

    if(file_id is None):
      # Next priority is to poll files that are not in the gsafile table at all
      # Most efficient way to find these is to do File LEFT OUTER JOIN GsaFile then select on lastpoll == NULL
      # Actually left table would be file join diskfile so that we can order by lastmod
      query = session.query(File).select_from(outerjoin(join(File, DiskFile), GsaFile))
      query = query.filter(DiskFile.canonical == True)
      query = query.filter(GsaFile.lastpoll == None)
      query = query.order_by(desc(DiskFile.lastmod)).limit(1)
      num = query.count()
      if(num > 0):
        f = query.first()
        file_id = f.id
        logger.info("Found file %d %s which has never been polled" % (f.id, f.filename))
        gf = GsaFile()
        gf.file_id = f.id
        gsainfo = CadcCRC.get_gsa_info(f.filename, gsa_user, gsa_pass)
        gf.md5sum = gsainfo['md5sum']
        gf.ingestdate = gsainfo['ingestdate']
        gf.lastpoll = datetime.datetime.now()
        session.add(gf)
        session.commit()

    if(file_id is None):
      # Didn't find anything, all up to date
      logger.info("All files up to date. Sleeping 10 seconds more")
      time.sleep(10)

    # Sleep a bit to prevent excessive GSA hammering
    time.sleep(0.4)

  except KeyboardInterrupt:
    loop=False

  except:
    string = traceback.format_tb(sys.exc_info()[2])
    string = "".join(string)
    logger.error("Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

  finally:
    session.close()

session.close()
if(options.lockfile):
  logger.info("Deleting Lockfile %s" % lockfile)
  os.unlink(lockfile)
logger.info("*********  poll_gsafiles.py - exiting at %s" % datetime.datetime.now())

