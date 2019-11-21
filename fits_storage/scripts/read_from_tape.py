import sys
import os
import re
import datetime
import time
import subprocess
import tarfile
import urllib.request, urllib.parse, urllib.error
from xml.dom.minidom import parseString

from fits_storage.orm import sessionfactory
#from fits_storage.fits_storage_config import *
from fits_storage import fits_storage_config
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.orm.tapestuff import TapeWrite, Tape, TapeFile, TapeRead
from fits_storage.utils.tape import get_tape_drive

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapedrive", action="store", type="string", default="/dev/nst0", dest="tapedrive", help="tapedrive to use.")
parser.add_option("--file-re", action="store", type="string", dest="filere", help="Regular expression used to select files to extract")
parser.add_option("--list-tapes", action="store_true", dest="list_tapes", help="only lists the tapes in TapeRead")
parser.add_option("--maxtars", action="store", type="int", dest="maxtars", help="Read a maximum of maxfiles tar archives")
parser.add_option("--maxgbs", action="store", type="int", dest="maxgbs", help="Stop at the end of the tarfile after we read maxgbs GBs")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    read_from_tape.py - starting up at %s" % datetime.datetime.now())

# Query the DB to find a list of files to extract
# This is a little non trivial, given that there are multiple identical
# copies of the file on several tapes and also that there can be multiple
# non identical version of the file on tapes too.
session = sessionfactory()
# Generate a list of the tapes that would be useful to satisfy this read
query = session.query(Tape).select_from(Tape, TapeWrite, TapeFile, TapeRead)
query = query.filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
query = query.filter(Tape.active == True).filter(TapeWrite.suceeded == True)
query = query.filter(TapeFile.filename == TapeRead.filename)
query = query.filter(TapeFile.md5 == TapeRead.md5)

tapes = query.all()

labels = []
if(len(tapes) == 0):
    logger.info("No tapes to be read, exiting")
    sys.exit(0)

for tape in tapes:
    labels.append(tape.label)
labels.sort()
logger.info("The following tapes contain requested files: %s" % labels)
for l in labels:
    logger.info("There is data to read on tape: %s" % l)

if(options.list_tapes):
    sys.exit(0)

try:
    # Make a FitsStorageTape object from class TapeDrive initializing the device and scratchdir
    td = get_tape_drive(options.tapedrive, fits_storage_config.fits_tape_scratchdir)
    label = td.readlabel()
    logger.info("You are reading from this tape: %s" % label)
    if label not in labels:
        logger.info("This tape does not contain files that were requested. Aborting")
        sys.exit(1)

    # OK, now we need to get a list of the filenums that contain files we want
    query = session.query(TapeWrite.filenum).select_from(Tape, TapeWrite, TapeFile, TapeRead)
    query = query.filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
    query = query.filter(Tape.active == True).filter(TapeWrite.suceeded == True)
    query = query.filter(TapeFile.filename == TapeRead.filename).filter(TapeFile.md5 == TapeRead.md5)
    query = query.filter(Tape.label == label)
    query = query.distinct()
    filenums = query.all()

    # Make a working directory and prepare the tapedrive
    td.mkworkingdir()
    td.cdworkingdir()
    td.setblk0()
    td.rewind()

    # Loop through the filenums
    tars = 0
    bytes = 0
    logger.info("Going to read from %d file numbers on this tape" % len(filenums))
    for filenum in filenums:
        logger.info("Going to read from file number %d" % filenum)
        tars = tars+1
        if(options.maxtars and (tars > options.maxtars)):
            logger.info("Read maxtars tar files. Stopping now")
            break
        if(options.maxgbs and ((bytes / 1.0E9) > options.maxgbs)):
            logger.info("Read maxgbs GBs. Stopping now")
            break
        # Fast forward the drive to that filenum
        logger.debug("Reading from filenumber %d" % filenum[0])
        td.skipto(filenum=filenum[0])

        # Query the filenames at the filenum and make a list of filenames
        query = session.query(TapeFile).select_from(Tape, TapeWrite, TapeFile, TapeRead)
        query = query.filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
        query = query.filter(Tape.active == True).filter(TapeWrite.suceeded == True)
        query = query.filter(TapeFile.filename == TapeRead.filename).filter(TapeFile.md5 == TapeRead.md5)
        query = query.filter(Tape.label == label)
        query = query.filter(TapeWrite.filenum == filenum[0])

        fileresults = query.all()
        logger.info("Going to extract %d files from this tar archive" % len(fileresults))
        filenames = []
        for thing in fileresults:
            filenames.append(thing.filename.encode())
            bytes += thing.size

        # A function to yeild all tarinfo objects and 'delete' all the read files from the taperead table
        blksize = 64*1024
        def fits_files(members):
            for tarinfo in members:
                if tarinfo.name in filenames:
                    session.query(TapeRead).filter(TapeRead.filename == tarinfo.name).delete()
                    session.commit()
                    logger.info("Reading file %s" % tarinfo.name)
                    yield tarinfo

        # Open the tarfile on the tape and extract the tarinfo object
        tar = tarfile.open(name=options.tapedrive, mode='r|', bufsize=blksize)
        tar.extractall(members=fits_files(tar))
        tar.close()

    # Are there any more files in TapeRead?
    taperead = session.query(TapeRead).all()
    if(len(taperead)):
        logger.info("There are more files to be read on different tapes")
    else:
        logger.info("All requested files have been read")

finally:
    td.cdback()
    session.close()
