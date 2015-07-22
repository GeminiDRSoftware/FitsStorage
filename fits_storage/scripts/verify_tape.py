import sys
import os
import datetime
import tarfile
from sqlalchemy import join
from sqlalchemy.orm.exc import NoResultFound

from fits_storage.orm import sessionfactory
from fits_storage.orm.tapestuff import Tape, TapeWrite, TapeFile

from fits_storage. import fits_storage_config
from fits_storage.utils.tape import TapeDrive
from fits_storage.utils.hashes import md5sum_size_fp

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--tapedrive", action="store", type="string", default="/dev/nst0", dest="tapedrive", help="tapedrive to use.")
parser.add_option("--verbose", action="store_true", dest="verbose", help="Make it print something to say 'this file is OK' for the files that are OK, as opposed to the normal mode that would only print something when there is a problem...")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

fits_storage_config.logname = "%s-%s.log" % (os.path.basename(sys.argv[0]), options.tapedrive.split('/')[-1])

from fits_storage.logger import logger, setdebug, setdemon
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

session = sessionfactory()


# Annouce startup
logger.info("*********    verify_tape.py - starting up at %s" % datetime.datetime.now())


try:
    # Make a FitsStorageTape object from class TapeDrive initializing the device and scratchdir
    td = TapeDrive(options.tapedrive, fits_storage_config.fits_tape_scratchdir)
    td.setblk0()
    label = td.readlabel()

    # Find the tape in the DB
    try:
        tape = session.query(Tape).filter(Tape.label==label).filter(Tape.active==True).one()
    except (NoResultFound):
        logger.error("The tape %s was not found in the DB." % label)
        sys.exit(1)

    # Make a list of errors found on the tape
    errors = []

    # Find all the tapewrite objects for it, loop through them in filenum order
    tw_list = session.query(TapeWrite).filter(TapeWrite.tape_id==tape.id).filter(TapeWrite.suceeded==True).order_by(TapeWrite.filenum).all()
    for tw in tw_list:
        # Send the tapedrive to this tapewrite
        try:
            logger.debug("Sending tape to filenumber %d" % tw.filenum)
            td.skipto(filenum=tw.filenum, fail=True)
        except (IOError):
            logger.error("Found file number in the database but not on tape at filenum: %s" % tw.filenum)
            errors.append(("File number not on tape at filenum = %s" % tw.filenum).encode())
            break
        logger.info("Reading the files from tape: %s, at file number: %d" % (label, tw.filenum))

        # Read all the fits files in the tar archive, one at a time, looping through and calculating the md5
        files_on_tape = []
        block = 64*1024
        try:
            # We have to open the tape drive independently so we've got something to close if the tar header read fails
            tdfileobj = open(options.tapedrive, 'rb')
            tar = tarfile.open(name=options.tapedrive, fileobj=tdfileobj, mode='r|', bufsize=block)
            for tar_info in tar:
                filename = tar_info.name
                if(options.verbose):
                    logger.info("Found file %s on tape." % filename)

                # Find the tapefile object
                try:
                    tf = session.query(TapeFile).filter(TapeFile.tapewrite_id==tw.id).filter(TapeFile.filename==filename).one()
                except (NoResultFound):
                    pass

                # Check whether this filename is in the DB
                if(tf):
                    files_on_tape.append(filename)
                    # Compare the tapefile object in the DB and the tarinfo object for the actual thing on tape
                    if(tar_info.size==tf.size):
                        logger.debug("Size matches in tape and DB for file: %s, in filenum: %d" % (tf.filename, tw.filenum))
                        # Calculate the md5 of the data on tape
                        f = tar.extractfile(tar_info)
                        (md5, size) = md5sum_size_fp(f)
                        f.close()
                    else:
                        logger.error("Size mismatch between tape and DB for file: %s, in filenum: %d" % (tf.filename, tw.filenum))
                        errors.append(("SIZE mismatch at filenum = %d, filename = %s" % (tw.filenum, tf.filename)).encode())
                        break
                    # Compare against the DB
                    if(md5 != tf.md5):
                        logger.error("md5 mismatch between tape and DB for file: %s, in filenum: %d" % (tf.filename, tw.filenum))
                        errors.append(("MD5 mismatch at filenum = %d, filename = %s" % (tw.filenum, tf.filename)).encode())
                    else:
                        logger.debug("md5 matches in tape and DB for file: %s, in filenum: %d" % (tf.filename, tw.filenum))
                else:
                    logger.error("File %s not found in DB." % filename)
                    errors.append(("File not in DB at filenum = %d, filename = %s" % (tw.filenum, tf.filename)).encode())
                    break
            tar.close()
        except (tarfile.ReadError):
            logger.error("Tar read error on open - most likely an empty tar file: tape %s, file number %d" % (label, tw.filenum))
        finally:
            tdfileobj.close()


        # Check whether we read everything in the DB
        files_in_DB = session.query(TapeFile).select_from(join(TapeFile, join(TapeWrite, Tape))).filter(TapeWrite.filenum==tw.filenum).filter(Tape.label==label).order_by(TapeFile.filename).all()
        for file in files_in_DB:
            if file.filename not in files_on_tape:
                logger.error("This file was in the database, but not on the tape: %s, in filenum: %d" % (file.filename, file.filenum))
                errors.append(("File not on Tape at filenum = %d, filename = %s" % (file.filenum, file.filename)).encode())

    # Print a list of all the errors found
    logger.info("List of Differences Found: %s" % errors)

    if(len(errors)):
        logger.error("There were verify errors - not updating lastverified")
    else:
        now = datetime.datetime.utcnow()
        logger.info("There were no verify errors - updating lastverified to: %s UTC" % now)
        tape.lastverified = now
        session.commit()

finally:
    td.cdback()
    session.close()

