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
import Cadc
import urllib2

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run as a background demon, do not generate stdout")
parser.add_option("--lockfile", action="store", dest="lockfile", help="Use this as a lockfile to limit instances")
parser.add_option("--authfile", action="store", dest="authfile", default="/home/fitsdata/.gsaauth", help="File containing the authentication credentials to the GSA")
parser.add_option("--bulk", action="store_true", dest="bulk", default=False, help="Bulk process files not in gsa table")

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
# Setup to Loop forever. loop is a global variable defined up top

# Strategy: 
#1) If a diskfile is not in the gsafile at all, then add it. Use NULLs if not in GSA.
while(loop):
  found_one = False
  try:
    # poll files that are not in the gsafile table at all
    # Most efficient way to find these is to do File LEFT OUTER JOIN GsaFile then select on lastpoll == NULL
    # Actually left table would be file join diskfile so that we can order by lastmod
    query = session.query(File).select_from(outerjoin(join(File, DiskFile), GsaFile))
    query = query.filter(DiskFile.canonical == True)
    query = query.filter(GsaFile.lastpoll == None)
    query = query.order_by(desc(DiskFile.lastmod))
    if(not options.bulk):
      query = query.limit(1)
    num = query.count()
    if(options.bulk):
      logger.info("Found %d files to bulk process" % num)
    if(num > 0):
      found_one = True
      fl = query.all()
      logger.debug("Got %d files to process" % len(fl))
      i=0
      for f in fl:
        i+=1
        file_id = f.id
        logger.info("Found file %d %s which has never been polled" % (f.id, f.filename))
        try:
          gf = GsaFile()
          gf.file_id = f.id
          logger.debug("Querying GSA for file %s" % f.filename)
          gsainfo = Cadc.get_gsa_info(f.filename, gsa_user, gsa_pass)
          logger.debug("Got md5: %s" % gsainfo['md5sum'])
          gf.md5 = gsainfo['md5sum']
          gf.ingestdate = gsainfo['ingestdate']
          gf.lastpoll = datetime.datetime.now()
          logger.debug("Adding new GsaFile")
          session.add(gf)
        except urllib2.URLError:
          logger.error("URLError - most likely connection timed out...")
          string = traceback.format_tb(sys.exc_info()[2])
          string = "".join(string)
          logger.error("Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

        # The Commit is slow when we have a big DB, so do this in batches if were in bulk mode
        if((not options.bulk) or ((i % 25)==0) or (num < 25)):
          logger.debug("Committing transaction")
          session.commit()
        logger.debug("Done")
        if(not loop):
          break

    #2) Now we want to poll files that might have been updated at the GSA since we last polled them.

    if(found_one == False):
      # Next priority is to (re-) poll non-eng files where the canonical diskfile lastmod is more recent than the gsafile date.
      # Don't poll any one given file more often than every 10 mins if it is from the last day
      ## - only poll a file if it is from the last day and it was polled more than 10 mins ago
      # Don't poll any one given file more often than every hour if it is from from more than a day ago
      ## - only poll a file if is is from the last 5 days and it was polled more than an hour ago
      # Don't poll any one given file more often that daily if it is from more than 5 days ago
      ## - only poll a file if it from more than 5 days ago and it was polled more than a day ago

      # say - only poll a file if was modified more than x hours ago and was polled more than 0.1x hours ago.

      # now() - Diskfile.lastmod gives time since last modification
      # now() - GsaFile.lastpoll gives time since last poll
      
      query = session.query(File).select_from(DiskFile, File, GsaFile, Header)
      query = query.filter(DiskFile.file_id == File.id).filter(GsaFile.file_id == File.id).filter(DiskFile.canonical == True).filter(Header.diskfile_id == DiskFile.id)
      query = query.filter(not_(Header.program_id.contains('ENG')))
      query = query.filter(Header.qa_state != 'Fail')
      query = query.filter(or_((DiskFile.lastmod > GsaFile.ingestdate), GsaFile.ingestdate == None))

      now = datetime.datetime.now()
      oneday = datetime.timedelta(days=1)
      query = query.filter(or_(((now-GsaFile.lastpoll) > (GsaFile.lastpoll-DiskFile.lastmod)), now-GsaFile.lastpoll > oneday))
      
      query = query.order_by(desc(DiskFile.lastmod)).limit(1)
      logger.debug("Query: %s" % query)
      num = query.count()
      if(num > 0):
        found_one = True
        f = query.first()
        file_id = f.id
        logger.info("Found file %d %s which has been modified since last polled" % (f.id, f.filename))
        gquery = session.query(GsaFile).filter(GsaFile.file_id == file_id)
        gf = gquery.one()
        logger.debug("Got GsaFile id %d file_id %d" % (gf.id, gf.file_id))
        gsainfo = Cadc.get_gsa_info(f.filename, gsa_user, gsa_pass)
        gf.md5 = gsainfo['md5sum']
        gf.ingestdate = gsainfo['ingestdate']
        gf.lastpoll = text('NOW()')
        session.commit()
        logger.debug("Updated lastpoll for id %d to %s" % (gf.id, gf.lastpoll))

    if(found_one == False):
      # Didn't find anything, all up to date
      logger.info("All files up to date. Sleeping 10 seconds more")
      time.sleep(10)

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

