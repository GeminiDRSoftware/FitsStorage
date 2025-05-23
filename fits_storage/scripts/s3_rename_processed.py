#! /usr/bin/env python3

import datetime

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.config import get_config

from fits_storage.db import sessionfactory
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.fulltextheader import FullTextHeader
from fits_storage.core.orm.header import Header

fsc = get_config()

if __name__ == "__main__":
    # Option Parsing
    from argparse import ArgumentParser
    # ------------------------------------------------------------------------------
    parser = ArgumentParser()

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run in the background, do not generate stdout")

    parser.add_argument("--dest", action="store", dest="dest", default=None,
                        help="Destination folder (prefix) to use")

    parser.add_argument("--by-filepre", action="store", dest="filepre",
                        default=None, help="Select files by (case sensitive) "
                                           "filename prefix. Note - filename, "
                                           "not S3 keyname")
    parser.add_argument("--iraf-bias", action="store_true", dest="irafbias",
                        default=False, help="IRAF processed BIAS selection")

    parser.add_argument("--current-path", action="store", dest="currentpath",
                        default=None, help="Select files on current path."
                                           "Leave blank to specify files in"
                                           "root folder only")

    parser.add_argument("--move", action="store_true", dest="move",
                        default=False, help="move files and update database")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("*** s3_rename_processed.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))


    def moveorlist(df, move=False, dest=None):
        if move:
            if dest is None:
                raise ValueError("Refusing to move to destination=None")
            oldkey = df.keyname
            newkey = f"{options.dest}/{df.filename}"
            logger.info(f"Rename S3 key: {oldkey} to {newkey}")
            try:
                s3.rename(oldkey, newkey)
                df.path = dest
            except Exception:
                logger.error("S3 rename failed", exc_info=True)
                raise
        else:
            logger.info(f"Filename: {df.filename} at path: {df.path}")

    if fsc.using_s3:
        from fits_storage.server.aws_s3 import Boto3Helper
        s3 = Boto3Helper()
    else:
        logger.error("Not an S3 configuration")
        exit()

    session = sessionfactory()

    if options.filepre == 'by-filepre':
        query = session.query(DiskFile).filter(DiskFile.present==True) \
            .filter(DiskFile.filename.startswith(options.filepre))

        if options.currentpath is None:
            query = query.filter(DiskFile.path=="")
        else:
            query = query.filter(DiskFile.path==options.currentpath)

        for df in query:
            try:
                moveorlist(df, move=options.move, dest=options.dest)
            except:
                break
        session.commit()

    if options.irafbias:
        # This is messy as we have to grep the fulltextheader to tell the
        # difference between DRAGONS and IRAF.
        query = session.query(Header).join(DiskFile) \
            .filter(DiskFile.present==True) \
            .filter(Header.observation_type=='BIAS') \
            .filter(Header.types.contains('PROCESSED'))

        if options.currentpath is None:
            query = query.filter(DiskFile.path=="")
        else:
            query = query.filter(DiskFile.path==options.currentpath)

        headers = query.all()
        logger.info(f"Found {len(headers)} candidates to check")
        dstring = "GBIAS   = 'Compatibility'      / For IRAF compatibility"
        istring = "GBIAS   = '20"

        for h in headers:
            df = h.diskfile
            try:
                fth = session.query(FullTextHeader) \
                    .filter(FullTextHeader.diskfile_id==df.id).one()
            except Exception:
                logger.info("Exception finding fulltextheader", exc_info=True)
                break
            if dstring in fth.fulltext:
                logger.debug(f"{df.filename} is DRAGONS - skipping")
                continue
            if istring not in fth.fulltext:
                logger.warning(f"{df.filename} not DRAGONS or IRAF - skipping")
                continue
            try:
                moveorlist(df, move=options.move, dest=options.dest)
                df.path=options.dest
            except:
                break
        session.commit()

