import sys
import os
import datetime
import tarfile
from bz2 import BZ2File
import hashlib

from tempfile import mkstemp

from sqlalchemy import func

from gemini_obs_db import sessionfactory
from fits_storage.orm.tapestuff import Tape, TapeWrite, TapeFile, TapeRead
from fits_storage.fits_storage_config import fits_tape_scratchdir
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.tape import get_tape_drive


class DeferredTarEntry:
    """
    This is a helper class for wrapping up information needed to create a new tar entry
    in the output tar stream.  We bundle these up as we read because creating and
    checksumming the bz2 files is expensive.  Once we have everything calculated and
    staged on disk, then we stream it all to the output tape drive at once.
    """
    def __init__(self, from_name, fits_name, tarinfo, filename, md5, size, compressed):
        self.from_name = from_name
        self.fits_name = fits_name
        self.tarinfo = tarinfo
        self.filename = filename
        self.md5 = md5
        self.size = size
        self.compressed = compressed


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--fromtapedrive", action="store", type="string", default="/dev/nst0", dest="fromtapedrive", help="tapedrive to read from.")
    parser.add_option("--totapedrive", action="store", type="string", default="/dev/nst0", dest="totapedrive", help="tapedrive to write to.")
    parser.add_option("--file-re", action="store", type="string", dest="filere", help="Regular expression used to select files to extract")
    parser.add_option("--list-tapes", action="store_true", dest="list_tapes", help="only lists the tapes in TapeRead")
    parser.add_option("--maxtars", action="store", type="int", dest="maxtars", help="Read a maximum of maxfiles tar archives")
    parser.add_option("--maxgbs", action="store", type="int", dest="maxgbs", help="Stop at the end of the tarfile after we read maxgbs GBs")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--compress", action="store_true", dest="compress",
                      help="Compress files in destination even if not compressed on source")
    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)


    # Annouce startup
    logger.info("*********    copy_to_new_tape.py - starting up at %s" % datetime.datetime.now())

    # Query the DB to find a list of files to extract
    # This is a little non trivial, given that there are multiple identical
    # copies of the file on several tapes and also that there can be multiple
    # non identical version of the file on tapes too.
    session = sessionfactory()
    # Generate a list of the tapes that would be useful to satisfy this read
    tapequery = (
        session.query(Tape).select_from(Tape, TapeWrite, TapeFile, TapeRead)
            .filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
            .filter(Tape.active == True).filter(TapeWrite.suceeded == True)
            .filter(TapeFile.filename == TapeRead.filename)
            .filter(TapeFile.md5 == TapeRead.md5)
        )

    if tapequery.count() == 0:
        logger.info("No tapes to be read, exiting")
        sys.exit(0)

    labels = []
    for tape in tapequery:
        labels.append(tape.label)
        s = (
            session.query(func.sum(TapeFile.size)).select_from(Tape, TapeWrite, TapeFile, TapeRead)
                .filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
                .filter(Tape.active == True).filter(TapeWrite.suceeded == True)
                .filter(TapeFile.filename == TapeRead.filename)
                .filter(TapeFile.md5 == TapeRead.md5)
                .filter(Tape.label == tape.label)
                .group_by(Tape)
                .one()
            )[0]
        logger.info("Tape %s contains %.2f GB to read" % (tape.label, s / 1.0E9))

    if options.list_tapes:
        sys.exit(0)

    try:
        # Make a FitsStorageTape object from class TapeDrive initializing the device and scratchdir
        fromtd = get_tape_drive(options.fromtapedrive, fits_tape_scratchdir)
        logger.info("Reading tape labels...")
        fromlabel = fromtd.readlabel()
        logger.info("You are reading from this tape: %s" % fromlabel)
        if fromlabel not in labels:
            logger.info("This tape does not contain files that were requested. Aborting")
            sys.exit(1)

        totd = get_tape_drive(options.totapedrive, fits_tape_scratchdir)
        tolabel = totd.readlabel()
        logger.info("You are writing to this tape: %s" % tolabel)

        # Find the tapes in the database
        # NOTE: None of those are used. Why?
        fromtape = session.query(Tape).filter(Tape.label == fromlabel).one()
        totape = session.query(Tape).filter(Tape.label == tolabel).one()

        # Make a working directory and prepare the tapedrives
        fromtd.mkworkingdir()
        fromtd.cdworkingdir()
        fromtd.setblk0()
        logger.debug("Rewinding tape %s" % fromlabel)
        fromtd.rewind()
        totd.workingdir = fromtd.workingdir
        totd.setblk0()
        logger.debug("Sending tape %s to eod" % tolabel)
        totd.eod()
        if(totd.eot()):
            logger.error("Tape %s in %s is at End of Tape. Tape is Full. Marking tape as full in DB and aborting" % (totape.label, totd.dev))
            totape.full = True
            session.commit()
            fromtd.cleanup()
            fromtd.cdback()
            session.close()
            sys.exit(1)

        # OK, now we need to get a list of the filenums that contain files we want
        filenums = (
            session.query(TapeWrite.filenum).select_from(Tape, TapeWrite, TapeFile, TapeRead)
                    .filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
                    .filter(Tape.active == True).filter(TapeWrite.suceeded == True)
                    .filter(TapeFile.filename == TapeRead.filename).filter(TapeFile.md5 == TapeRead.md5)
                    .filter(Tape.label == fromlabel)
                    .distinct()
            )

        # Loop through the filenums
        bytes = 0
        logger.info("Going to read from %d file numbers on this tape" % filenums.count())
        for tars, (filenum,) in enumerate(filenums, 1):
            logger.info("Going to read from file number %d" % filenum)

            if(options.maxtars and (tars > options.maxtars)):
                logger.info("Read maxtars tar files. Stopping now")
                break

            if(options.maxgbs and ((bytes / 1.0E9) > options.maxgbs)):
                logger.info("Read maxgbs GBs. Stopping now")
                break

            # Fast forward the drive to that filenum
            logger.debug("Seeking to filenumber %d" % filenum)
            fromtd.skipto(filenum=filenum)

            # Query the filenames at the filenum and make a list of filenames
            query = session.query(TapeFile).select_from(Tape, TapeWrite, TapeFile, TapeRead)
            query = query.filter(Tape.id == TapeWrite.tape_id).filter(TapeWrite.id == TapeFile.tapewrite_id)
            query = query.filter(Tape.active == True).filter(TapeWrite.suceeded == True)
            query = query.filter(TapeFile.filename == TapeRead.filename).filter(TapeFile.md5 == TapeRead.md5)
            query = query.filter(Tape.label == fromlabel)
            query = query.filter(TapeWrite.filenum == filenum)

            fileresults = query.all()
            logger.info("Going to copy %d files from this tar archive" % len(fileresults))
            filenames = set()
            frbackref = {}

            for i, fr in enumerate(fileresults):
                #encoded_name = fr.filename.encode()
                #filenames.add(encoded_name)
                filenames.add(fr.filename)
                bytes += fr.size
                #frbackref[encoded_name] = fr
                frbackref[fr.filename] = fr

            # Prepare to write to the new tape
            # Update tape first/lastwrite
            logger.debug("Updating tape record for tape label %s" % totape.label)
            if totape.firstwrite == None:
                totape.firstwrite = datetime.datetime.utcnow()
            totape.lastwrite = datetime.datetime.utcnow()
            session.commit()

            # Create tapewrite record
            logger.debug("Creating TapeWrite record for tape %s..." % totape.label)
            tw = TapeWrite()
            tw.tape_id = totape.id
            session.add(tw)
            session.commit()
            # Update tapewrite values pre-write
            tw.beforestatus = totd.status()
            tw.filenum = totd.fileno()
            tw.startdate = datetime.datetime.utcnow()
            tw.hostname = os.uname()[1]
            tw.tapedrive = totd.dev
            tw.suceeded = False
            session.commit()
            logger.debug("... TapeWrite id=%d, filenum=%d" % (tw.id, tw.filenum))

            # Write the tape.
            bytecount = 0
            blksize = 64 * 1024
            totarok = True


            # Create the tarfile on the write tape
            logger.info("Creating tar archive on tape %s on drive %s" % (totape.label, totd.dev))
            try:
                totar = tarfile.open(name=totd.target(), mode='w|', bufsize=blksize)
            except:
                logger.error("Error opening tar destination archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                tarok = False

            # Open the tarfile on the read tape
            fromtar = tarfile.open(name=fromtd.target(), mode='r|', bufsize=blksize)

            # Loop through the tar file. Don't delete from the to-do lists until we sucessfully close the files
            done = []

            # If we are generating .bz2 files, we generate them all first so the tape drive doesn't get tortured
            deferred = list()
            for tarinfo in fromtar:
                normalized_from_name = tarinfo.name
                if normalized_from_name.lower().endswith(".bz2"):
                    normalized_from_name = normalized_from_name[:-4]
                if normalized_from_name in filenames:
                    logger.info("Processing file %s" % tarinfo.name)
                    # Re-find the tapefile instance
                    tf = frbackref[normalized_from_name]
                    #if tf.filename.encode() != tarinfo.name:
                    if tf.filename != normalized_from_name:
                        logger.error("tapefile instance index dereference problem! Skipping")
                        break
                    #for thing in fileresults:
                        #if thing.filename.encode() == tarinfo.name:
                            #tf = thing
                            #break
                    logger.debug("Found old tapefile: id=%d; filename=%s" % (tf.id, tf.filename))
                    f = fromtar.extractfile(tarinfo)

                    # Check for compression
                    file_md5 = tf.md5
                    file_size = tf.size
                    file_compressed = tf.compressed
                    file_filename = tarinfo.name
                    if not tf.compressed and options.compress:
                        file_filename = "%s.bz2" % tarinfo.name
                        stagefilename = mkstemp(dir=totd.workingdir)[1]
                        bzf = BZ2File(stagefilename, "w")
                        bzf.write(f.read())
                        bzf.flush()
                        bzf.close()

                        m = hashlib.md5()
                        bz2data = open(stagefilename, "rb")
                        data = bz2data.read()
                        m.update(data)
                        file_md5 = m.hexdigest()
                        file_size = len(data)
                        file_compressed = True

                        # make sure tar points at our bzipped data now
                        bz2data.close()
                        f.close()
                    else:
                        stagefilename = mkstemp(dir=totd.workingdir)[1]
                        stagefile = open(stagefilename, "wb")
                        stagefile.write(f.read())
                        stagefile.flush()
                        stagefile.close()
                        f.close()
                    # Add the file to the new tar archive.
                    # For some reason we need to construct the TarInfo object manually.
                    # I guess because it has the header from the other tarfile in it otherwise
                    # Just copy the public data members over and let it sort out the internals itself
                    newtarinfo = tarfile.TarInfo(file_filename)
                    newtarinfo.size = file_size  # tarinfo.size
                    newtarinfo.mtime = tarinfo.mtime
                    newtarinfo.mode = tarinfo.mode
                    newtarinfo.type = tarinfo.type
                    newtarinfo.uid = tarinfo.uid
                    newtarinfo.gid = tarinfo.gid
                    newtarinfo.uname = tarinfo.uname
                    newtarinfo.gname = tarinfo.gname

                    deferred_tar_entry = DeferredTarEntry(from_name = tarinfo.name,
                                                          fits_name= normalized_from_name,
                                                          tarinfo=newtarinfo,
                                                          filename=stagefilename, md5=file_md5,
                                                          size=file_size, compressed=file_compressed)
                    deferred.append(deferred_tar_entry)
                else:
                    logger.debug("Skipping over file that's not required: %s" % tarinfo.name)
                if len(filenames) == 0:
                    logger.info("Got everything we need from this tar archive, stopping reading it now")
                    break

            for deferred_tar_entry in deferred:
                tf = frbackref[deferred_tar_entry.fits_name]
                try:
                    newtarinfo = deferred_tar_entry.tarinfo
                    filedata = open(deferred_tar_entry.filename, "rb")
                    logger.debug("Copying data of file %s" % newtarinfo.name)
                    totar.addfile(newtarinfo, filedata)
                    filedata.close()
                    # Add the file to the done list
                    done.append(deferred_tar_entry.from_name)
                except:
                    logger.error("Error adding file to tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                    logger.info("Probably the tape filled up - Marking tape as full in the DB - label: %s" % totape.label)
                    totape.full = True
                    session.commit()
                    totarok = False
                    break

                # Create a new tapefile for the new copy in the new tapewrite and add to DB
                logger.debug("Creating new tapefile object and adding to DB")
                ntf = TapeFile()
                ntf.tapewrite_id = tw.id
                ntf.filename = tf.filename
                # ntf.ccrc = tf.ccrc
                ntf.md5 = deferred_tar_entry.md5
                ntf.lastmod = tf.lastmod
                ntf.size = deferred_tar_entry.size
                ntf.data_size = tf.data_size
                ntf.data_md5 = tf.data_md5
                ntf.compressed = deferred_tar_entry.compressed
                session.add(ntf)
                session.commit()

                # Keep a running total of bytes written
                bytecount += ntf.size

            # Close the tar archives and update the tapewrite etc
            try:
                logger.info("Closing both tar archives")
                fromtar.close()
                totar.close()
                # Delete the files we processed from the to-do lists
                for fn in done:
                    logger.debug("Deleting %s from taperead" % fn)
                    session.query(TapeRead).filter(TapeRead.filename == fn).delete()
                    session.commit()
                    filenames.remove(fn)

            except:
                logger.error("Error closing tar archive - Exception: %s : %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                totarok = False

            logger.debug("Updating tapewrite record")
            tw.enddate = datetime.datetime.utcnow()
            logger.debug("Succeeded: %s" % totarok)
            tw.suceeded = totarok
            tw.afterstatus = totd.status()
            tw.size = bytecount
            session.commit()

            # Cleaning up staging files
            for deferred_tar_entry in deferred:
                os.remove(deferred_tar_entry.filename)

            # If the previous tar failed, we should bail out now
            if not totarok:
                logger.error("Previous Tar operation failed, stopping now")
                break

        # Are there any more files in TapeRead?
        taperead = session.query(TapeRead).all()
        if len(taperead):
            logger.info("There are more files to be read on different tapes")
        else:
            logger.info("All requested files have been read")

    finally:
        fromtd.cdback()
        session.close()
