#!/usr/bin/env python3

import os
import datetime
import tarfile

from sqlalchemy import select
from argparse import ArgumentParser

from sqlalchemy.exc import NoResultFound

from fits_storage.db import sessionfactory
from fits_storage.server.orm.tapestuff import Tape, TapeWrite, TapeFile
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.server.tapeutils import TapeDrive
from fits_storage.config import get_config

if __name__ == "__main__":
    parser = ArgumentParser(prog='replicate_tape.py',
                            description="Used to duplicate tapes")
    parser.add_argument("--fromtapedrive", action="store",
                      dest="fromtapedrive", help="tapedrive to read from.")
    parser.add_argument("--totapedrive", action="store",
                      dest="totapedrive", help="tapedrive to write to.")
    parser.add_argument("--debug", action="store_true", dest="debug",
                      help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(args.debug)
    setdemon(args.demon)

    # Announce startup
    logger.info("***   replicate_tape.py - starting up at %s",
                datetime.datetime.now())

    fsc = get_config()
    blksize = 64 * 1024

    # Method:
    # Loop through the TapeWrites on fromtape, read files and write to a new
    # TapeWrite on totape.

    logger.info("Reading tape labels...")
    fromtd = TapeDrive(args.fromtapedrive, fsc.fits_tape_scratchdir)
    fromlabel = fromtd.readlabel()
    logger.info(f"You are reading from this tape: {fromlabel}")
    totd = TapeDrive(args.totapedrive, fsc.fits_tape_scratchdir)
    tolabel = totd.readlabel()
    logger.info("You are writing to this tape: %s" % tolabel)

    session = sessionfactory()

    # Get the tape ORM instances
    try:
        stmt = select(Tape).where(Tape.label == fromlabel)
        fromtape = session.execute(stmt).scalars().one()
        stmt = select(Tape).where(Tape.label == tolabel)
        totape = session.execute(stmt).scalars().one()
    except NoResultFound:
        logger.error("Could not find one of the tapes in the database. Exiting")
        exit(1)

    # Get a list of tapewrites on from tape, in order of filenum.
    # Do not require the tape to be active.
    stmt = (select(TapeWrite).select_from(TapeWrite, Tape)
            .where(TapeWrite.tape_id == Tape.id)
            .where(TapeWrite.succeeded is True)
            .where(Tape.id == fromtape.id)
            .order_by(TapeWrite.filenum))
    tapewrites = session.execute(stmt).scalars()
    logger.info(f"Found {len(tapewrites)} tapewrites to copy")

    for tw in tapewrites:
        # Send the tapes to position
        logger.info(f"Going to read from file number {tw.filenum}")
        fromtd.skipto(filenum=tw.filenum)
        totd.eod()

        # Open the tarfile on the read tape
        fromtar = tarfile.open(name=fromtd.dev, mode='r|', bufsize=blksize)

        # Create the tarfile on the write tape
        logger.info(f"Creating tar archive on {totape.label} on {totd.dev}")
        try:
            totar = tarfile.open(name=totd.dev, mode='w|',
                                 bufsize=blksize)
        except:
            logger.error("Exception opening tar destination archive, aborting",
                         exc_info=True)
            exit(1)

        # Create tapewrite record
        logger.debug(f"Creating TapeWrite record for tape {totape.label}...")
        ntw = TapeWrite()
        ntw.tape_id = totape.id
        session.add(ntw)
        session.commit()
        # Fill pre-write values
        ntw.beforestatus = totd.status()
        ntw.filenum = totd.fileno()
        ntw.startdate = datetime.datetime.utcnow()
        ntw.hostname = os.uname()[1]
        ntw.tapedrive = totd.dev
        ntw.succeeded = False
        session.commit()
        logger.debug("... TapeWrite id=%d, filenum=%d", (ntw.id, ntw.filenum))

        # Update totape record first/lastwrite
        logger.debug(f"Updating tape record for tape label {totape.label}")
        if totape.firstwrite is None:
            totape.firstwrite = datetime.datetime.utcnow()
        totape.lastwrite = datetime.datetime.utcnow()
        session.commit()

        # Keep a bytecount running total
        bytecount = 0

        # Loop through the tar file
        for tarinfo in fromtar:
            # Get a file like object on the actual data
            flo = fromtar.extractfile(tarinfo)

            # Add the file to the new tar archive. We need to construct the
            # TarInfo object manually because it has the header from the other
            # tarfile in it. Just copy the public data members over and let it
            # sort out the internals itself
            newtarinfo = tarfile.TarInfo(tarinfo.name)
            newtarinfo.size = tarinfo.size
            newtarinfo.mtime = tarinfo.mtime
            newtarinfo.mode = tarinfo.mode
            newtarinfo.type = tarinfo.type
            newtarinfo.uid = tarinfo.uid
            newtarinfo.gid = tarinfo.gid
            newtarinfo.uname = tarinfo.uname
            newtarinfo.gname = tarinfo.gname

            # Write it to the to tar file
            try:
                totar.addfile(newtarinfo, flo)
                flo.close()
            except:
                logger.error("Exception adding file to tar archive",
                             exc_info=True)
                logger.info("Probably the tape filled up - Marking tape "
                            "as full in the DB - label: %s", totape.label)
                totape.full = True
                session.commit()
                totarok = False
                break
            bytecount += tarinfo.size

            # Find the TapeFile entry for the file we are reading
            stmt = (select(TapeFile)
                    .where(TapeFile.tapewrite_id == tw.id)
                    .where(TapeFile.filename == tarinfo.name))
            tf = session.execute(stmt).scalars().one()

            # Create a new tapefile record for the new copy in the new tapewrite
            # and add to DB
            logger.debug("Creating new tapefile object and adding to DB")
            ntf = TapeFile()
            ntf.tapewrite_id = ntw.id
            ntf.filename = tf.filename
            ntf.md5 = tf.md5
            ntf.lastmod = tf.lastmod
            ntf.size = tf.size
            ntf.data_size = tf.data_size
            ntf.data_md5 = tf.data_md5
            ntf.compressed = tf.compressed
            session.add(ntf)
            session.commit()

        # Close the tar archives and update the tapewrite etc
        try:
            logger.info("Closing both tar archives")
            fromtar.close()
            totar.close()
        except:
            logger.error("Error closing tar archive, aborting", exc_info=True)
            exit(1)

        logger.debug("Updating tapewrite record")
        ntw.enddate = datetime.datetime.utcnow()
        ntw.succeeded = True
        ntw.afterstatus = totd.status()
        ntw.size = bytecount
        session.commit()

    logger.info("Processed all tapewrites. ")
    logger.info("***   replicate_tape.py - exiting normally at %s",
                datetime.datetime.now())