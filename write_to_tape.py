import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
from FitsStorageConfig import *
from FitsStorageLogger import *
from FitsStorageUtils import *
from FitsStorageTape import TapeDrive
import CadcCRC
import os
import re
import datetime
import time
import subprocess
import tarfile
import urllib

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--diskserver", action="store", type="string", dest="diskserver", default="fits", help="The Fits Storage Disk server to get the files from")
parser.add_option("--selection", action="store", type="string", dest="selection", help="the file selection criteria to use. This is a / separated list like in the URLs. Can be a date or daterange for example")
parser.add_option("--tapedrive", action="store", type="string", dest="tapedrive", help="tapedrive to use")
parser.add_option("--tape-label", action="store", type="string", dest="tape_label", help="tape label of tape")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  write_to_tape.py - starting up at %s" % datetime.datetime.now())

# Get the list of files to put on tape from the server
url = "http://" + options.diskserver + "/xmlfilelist/" + options.selection
logger.debug("file list url: %s" % url)

u = urllib.urlopen(url)
xml = u.read()
u.close()

dom = parseString(xml)
files = []
totalsize=0
for fe in dom.getElementsByTagName("file"):
  dict = {}
  dict['filename']=fe.getElementsByTagName("filename")[0].childNodes[0].data
  dict['size']=fe.getElementsByTagName("size")[0].childNodes[0].data
  dict['ccrc']=fe.getElementsByTagName("ccrc")[0].childNodes[0].data
  dict['lastmod']=fe.getElementsByTagName("lastmod")[0].childNodes[0].data
  files.append(dict)
  totalsize += size

numfiles = len(files)
logger.info("Got %d files to write to tape" % numfiles)
logger.info("Total size is %.2f GB" % (totalsize / 1.0E9))
if(numfiles == 0):
  logger.info("Exiting - no files")
  exit(0)
 
td = TapeDrive(options.tapedrive, fits_tape_scratchdir)

session = sessionfactory()

# Get the tape object for this tape label
logger.debug("Finding tape record in DB")
query = session.query(Tape).filter(Tape.label == options.tape_label).filter(Tape.active == True)
if(query.count() == 0):
  logger.error("Could not find active tape with label %s" % options.tape_label)
  sys.exit(1)
if(query.count() > 1):
  logger.error("Multiple active tapes with label %s:" % options.tape_label)
  sys.exit(1)
tape = query.one()
logger.debug("Found tape id: %d, label: %s" % (tape.id, tape.label))

# Check the tape label in the drive
logger.debug("Checking tape label in drive")
if(td.online() == False):
  logger.error("No tape in drive")
  sys.exit(1)
label = td.readlabel()
if(options.tape_label):
  if(label != options.tape_label):
    logger.error("Label of tape in drive does not match label given.")
    logger.error("Tape in Drive: %s; Tape specified: %s" % (label, options.tape_label))
    sys.exit(1)
logger.info("Found tape in drive with label: %s" % label)
    
# Position Tape
if(not options.dryrun):
  logger.info("Positioning Tape")
  td.setblk0()
  td.eod(fail=True)

# Copy the files to the local scratch, and check CRCs.
logger.info("Fetching files to local disk")
td.cdworkingdir()
for f in files:
    filename = f['filename']
    size = int(f['size'])
    ccrc = f['ccrc']
    url="http://%s/file/%s" % (options.diskserver, filename)
    logger.debug("Fetching file: %s from %s" % (filename, url))
    retcode=subprocess.call(['/usr/bin/curl', '-b', 'gemini_fits_authorization=good_to_go', '-O', '-f', url])
    if(retcode):
      # Curl command failed. Bail out
      logger.error("Fetch failed for url: %s" % url)
      td.cdback()
      td.cleanup()
      sys.exit(1)
    else:
      # Curl command suceeded.
      # Check the CRC of the file we got against the DB
      filecrc = CadcCRC.cadcCRC(filename)
      if(filecrc != ccrc):
        logger.error("CRC mismatch for file %s: file: %s, database: %s" % (filename, filecrc, ccrc))
        td.cdback()
        td.cleanup()
        sys.exit(1)
       # Check the md5sum of the file we got against the DB
       # Actually, the DB doesn't have md5 yet, so just calcultate it here for use later.
       md5sum = CadcCRC.md5sum(filename)
       f['md5sum'] = md5sum

  logger.info("All files fetched OK")
      
    
  if(not options.dryrun):
    # Update tape first/lastwrite
    logger.debug("Updating tape record")
    if(not tape.firstwrite):
      tape.firstwrite = datetime.datetime.now()
    tape.lastwrite = datetime.datetime.now()
    session.commit()

  if(not options.dryrun):
    logger.debug("Creating TapeWrite record")
    # Create tapewrite record
    tw = FitsStorage.TapeWrite()
    tw.tape_id = tape.id
    session.add(tw)
    session.commit()
    # Update tapewrite values pre-write
    tw.beforestatus = td.status()
    tw.filenum = td.fileno()
    tw.startdate = datetime.datetime.now()
    tw.hostname = os.uname()[1]
    tw.tapedrive = td.dev
    tw.suceeded = False
    session.commit()

  if(not options.dryrun):
    tarok = True
    # Write the tape.
    bytecount = 0
    blksize = 64 * 1024
    logger.info("Creating tar archive")
    try:
      tar = tarfile.open(name=td.dev, mode='w|', bufsize=blksize)
    except:
      tarok = False
    for f in files:
      filename = f['filename']
      size = int(f['size'])
      ccrc = f['ccrc']
      md5 = f['md5']
      lastmod = f['lastmod']
      logger.info("Adding %s to tar file" % filename)
      try:
        tar.add(filename)
      except:
        tarok = False
      # Create the TapeFile entry and add to DB
      tapefile = FitsStorage.TapeFile()
      tapefile.tapewrite_id = tw.id
      tapefile.filename = filename
      tapefile.ccrc = ccrc
      tapefile.md5 = md5
      tapefile.lastmod = lastmod
      tapefile.size = size
      session.add(tapefile)
      session.commit()
      # Keep a running total of bytes written
      bytecount += size
    logger.info("Completed writing tar archive")
    logger.info("Wrote %d bytes" % bytecount)
    try:
      tar.close()
    except:
      tarok = False

  if(not options.dryrun):
    # update records post-write
    logger.debug("Updating tapewrite record")
    tw.enddate = datetime.datetime.now()
    tw.suceeded = tarok
    tw.afterstatus = td.status()
    tw.size = bytecount
    session.commit()
   
session.close()
logger.info("*** write_to_tape exiting normally at %s" % datetime.datetime.now())

