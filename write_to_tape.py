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

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="file_re", help="python regular expression string to select files by. Special values are today, twoday, fourday to include only files from today, the last two days, or the last four days respectively (days counted as UTC days)")
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

td = TapeDrive(fits_tape_device, fits_tape_scratchdir)

file_re = options.file_re

session = sessionfactory()
# Need to select the diskfiles we are going to write, and track the size
query=session.query(DiskFile).select_from(join(File, DiskFile)).filter(DiskFile.present==True).order_by(File.filename)
squery=session.query(func.sum(DiskFile.size)).select_from(join(File, DiskFile)).filter(DiskFile.present==True)

if(file_re):
  logger.info("selecting files that match pattern: %s" % file_re)
  searchstring = '%'+file_re+'%'
  query = query.filter(File.filename.like(searchstring))
  squery = squery.filter(File.filename.like(searchstring))

num = query.count()
if(num):
  size = squery.one()[0]
  logger.info("found %d files totalling %.2f GB" % (num, size/1.0E9))
  diskfiles = query.all()
  if(options.dryrun):
    for diskfile in diskfiles:
      logger.info("Filename: %s" % diskfile.file.filename)
  else:
    # Get the tape object for this tape label
    query = session.query(Tape).filter(Tape.label == options.label).filter(Tape.active == True)
    if(query.count() == 0):
      logger.error("Could not find active tape with label %s" % options.label)
      sys.exit(1)
    if(query.count() > 1):
      logger.error("Multiple active tapes with label %s:" % options.label)
      sys.exit(1)
    tape = query.one()

    # Check the tape label in the drive
    if(td.online() == False):
      logger.error("No tape in drive")
      sys.exit(1)
    label = td.readlabel()
    if(label != options.tape_label):
      logger.error("Label of tape in drive does not match label given.")
      logger.error("Tape in Drive: %s; Tape specified: %s" % (label, options.tape_label))
      sys.exit(1)
    
    # Position Tape
    td.setblk0()
    td.eod(fail=True)

    # Copy the files to the local scratch, and check CRCs.
    td.cdwordingdir()
    for diskfile in diskfiles:
      filename = diskfile.file.filename
      url="http://%s/file/%s" % fits_servername, filename
      logger.debug("Fetching file: %s" % filename)
      retcode=subprocess.call(['/usr/bin/curl', '-b', 'gemini_fits_authorization=good_to_go', '-O', '-f', url)
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
        dbcrc = diskfile.ccrc
        if(filecrc != dbcrc):
          logger.error("CRC mismatch for file %s" % filename)
          td.cdback()
          td.cleanup()
          sys.exit(1)

    logger.info("All files fetched OK")
       
    
    # Update tape first/lastwrite
    if(not tape.firstwrite):
      tape.firstwrite(datetime.datetime.now())
    tape.lastwrite(datetime.datetime.now())
    session.commit()

    # Create tapewrite record
    tw = FitsStorage.TapeWrite
    tw.tape_id = tape.id
    session.add(tw)
    session.commit()
    tw.beforestatus = td.status()
    tw.fileno = td.fileno()
    tw.startdata = datetime.datetime.now()
    tw.hostname = os.uname[1]
    tw.tapedrive = td.dev
    tw.suceeded = False
    session.commit()

    # Write the tape.
    blksize = 64 * 1024
    logger.info("Creating tar archive")
    tar = tarfile.open(name=self.dev, mode='w|', bufsize=blksize)
    for diskfile in diskfiles:
      filename = diskfile.file.filename
      logger.info("Adding %s to tar file" % filename)
      tar.add(filename)
      # Create the TapeFile entry and add to DB
      tapefile = FitsStorage.TapeFile
      tapefile.tapewrite_id = tw.id
      tapefile.diskfile_id = diskfile.id
      session.add(tapefile)
      session.commit()
    logger.info("Completed writing tar archive")
    tar.close()

    # update records
    tw.enddate = datetime.datetime.now()
    tw.suceeded = True
    tw.afterstatus = td.status()
    session.commit()
    
else:
  logger.info("no files found")

session.close()
logger.info("*** write_to_tape exiting normally at %s" % datetime.datetime.now())

