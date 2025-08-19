#!/usr/bin/env python3

import sys
import datetime
import tarfile
from sqlalchemy import func
from optparse import OptionParser

from fits_storage.db import sessionfactory
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.orm.tapestuff import TapeWrite, Tape, TapeFile, \
    TapeRead
from fits_storage.server.tapeutils import TapeDrive
from fits_storage.config import get_config

# Option Parsing
parser = OptionParser()
parser.add_option("--tapedrive", action="store", type="string",
                  dest="tapedrive", help="tapedrive to use.")
parser.add_option("--file-re", action="store", type="string", dest="filere",
                  help="Regular expression used to select files to extract")
parser.add_option("--list-tapes", action="store_true", dest="list_tapes",
                  help="lists the tapes that meet the read request")
parser.add_option("--maxtars", action="store", type="int", dest="maxtars",
                  help="Read a maximum of maxfiles tar archives")
parser.add_option("--maxgbs", action="store", type="int", dest="maxgbs",
                  help="Stop at the end of the tarfile after maxgbs GBs")
parser.add_option("--dryrun", action="store_true", dest="dryrun",
                  help="Dry Run - do not actually do anything")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Make separate log files per tape drive
if options.tapedrive:
    setlogfilesuffix(options.tapedrive.split('/')[-1])

# Announce startup
logger.info("***   read_from_tape.py - starting up at %s",
            datetime.datetime.now())

# Query the DB to find a list of files to extract This is a little
# non-trivial, given that there are multiple identical copies of the file on
# several tapes and also that there can be multiple non-identical version of
# the file on tapes too.
session = sessionfactory()
fsc = get_config()

if options.list_tapes:
    # Generate a list of the tapes that would be useful to satisfy this read
    query = session.query(Tape)\
        .select_from(Tape, TapeWrite, TapeFile, TapeRead)\
        .filter(Tape.id == TapeWrite.tape_id)\
        .filter(TapeWrite.id == TapeFile.tapewrite_id)\
        .filter(Tape.active == True).filter(TapeWrite.succeeded == True)\
        .filter(TapeFile.filename == TapeRead.filename)\
        .filter(TapeFile.md5 == TapeRead.md5)

    tapes = query.all()

    if len(tapes) == 0:
        logger.info("No tapes to be read, exiting")
        sys.exit(0)

    labels = []
    for tape in tapes:
        labels.append(tape.label)
        labels.sort()
        logger.info("The following tapes contain requested files: %s", labels)
        for l in labels:
            query = session.query(func.sum(TapeFile.size))\
                .select_from(Tape, TapeWrite, TapeFile, TapeRead)\
                .filter(Tape.id == TapeWrite.tape_id)\
                .filter(TapeWrite.id == TapeFile.tapewrite_id)\
                .filter(Tape.active == True)\
                .filter(TapeWrite.succeeded == True)\
                .filter(TapeFile.filename == TapeRead.filename)\
                .filter(TapeFile.md5 == TapeRead.md5)\
                .filter(Tape.label == l)
            sumsize = query.one()[0]
            gbs = float(sumsize) / 1E9
            logger.info("There are %.1f GB to read on tape %s", gbs, l)

    # If all we're doing is listing tapes, stop here.
    sys.exit(0)

try:
    td = TapeDrive(options.tapedrive, fsc.fits_tape_scratchdir)
    label = td.readlabel()
    logger.info("You are reading from this tape: %s", label)

    # Now we need to get a list of the filenums (tapewrites) that contain
    # files we want
    query = session.query(TapeWrite)\
        .select_from(Tape, TapeWrite, TapeFile, TapeRead)\
        .filter(Tape.id == TapeWrite.tape_id)\
        .filter(TapeWrite.id == TapeFile.tapewrite_id)\
        .filter(Tape.active == True).filter(TapeWrite.succeeded == True)\
        .filter(TapeFile.filename == TapeRead.filename)\
        .filter(TapeFile.md5 == TapeRead.md5)\
        .filter(Tape.label == label)\
        .distinct().order_by(TapeWrite.filenum)
    tws = query.all()

    # Make a working directory and prepare the tapedrive
    td.mkworkingdir()
    td.cdworkingdir()
    td.setblk0()
    td.rewind()

    # Loop through the tapewrites (filenums)
    tars = 0
    bytes = 0
    logger.info("Going to read from %d file numbers on this tape", len(tws))
    for tw in tws:
        filenum = tw.filenum
        tars += 1
        if options.maxtars and (tars > options.maxtars):
            logger.info("Read maxtars tar files. Stopping now")
            break
        if options.maxgbs and ((bytes / 1.0E9) > options.maxgbs):
            logger.info("Read maxgbs GBs. Stopping now")
            break

        logger.info("Going to read from file number %d", filenum)
        # Fast-forward the drive to that filenum
        td.skipto(filenum=filenum)

        # Query the filenames at the filenum and make a list of filenames
        query = session.query(TapeFile)\
            .select_from(Tape, TapeWrite, TapeFile, TapeRead)\
            .filter(Tape.id == TapeWrite.tape_id)\
            .filter(TapeWrite.id == TapeFile.tapewrite_id)\
            .filter(Tape.active == True).filter(TapeWrite.succeeded == True)\
            .filter(TapeFile.filename == TapeRead.filename)\
            .filter(TapeFile.md5 == TapeRead.md5)\
            .filter(Tape.label == label)\
            .filter(TapeWrite.filenum == filenum)

        fileresults = query.all()
        logger.info("Going to extract %d files from this tar archive",
                    len(fileresults))
        filenames = []
        for thing in fileresults:
            filenames.append(thing.filename)
            bytes += thing.size

        logger.debug("filenames: %s", filenames)

        # Keep a list of completed files to delete from taperead. Doing this
        # on the fly slows things down too much with the faster tapedrives
        completed = []

        # A function to yeild all tarinfo objects and keep a list of completed
        # filenames
        def fits_files(members):
            for tarinfo in members:
                logger.debug("In fits_files, name %s" % tarinfo.name)
                if tarinfo.name in filenames:
                    completed.append(tarinfo.name)
                    logger.info("Reading file %s", tarinfo.name)
                    yield tarinfo

        # Open the tarfile on the tape and extract the tarinfo object
        blksize = 64*1024
        tar = tarfile.open(name=options.tapedrive, mode='r|', bufsize=blksize)
        tar.extractall(members=fits_files(tar), filter='tar')
        tar.close()

        # Delete the completed files from taperead
        for filename in completed:
            session.query(TapeRead) \
                    .filter(TapeRead.filename == filename).delete()
        session.commit()

        # Are there any more files in TapeRead?
        query = session.query(TapeRead)
        taperead = query.all()

        if len(taperead):
            logger.info("There are more files to be read on different tapes")
        else:
            logger.info("All requested files have been read")
finally:
    td.cdback()
    session.close()
