#! /usr/bin/env python3

import datetime

from fits_storage.cal.orm import Gmos
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.config import get_config

from fits_storage.db import sessionfactory
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.fulltextheader import FullTextHeader
from fits_storage.core.orm.header import Header
from fits_storage.server.orm.glacier import Glacier

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

    parser.add_argument("--filepre", action="store", dest="filepre",
                        default=None, help="Select files by (case sensitive) "
                                           "filename prefix. Note - filename, "
                                           "not S3 keyname")

    parser.add_argument("--filelike", action="store", dest="filelike",
                        default=None, help="select filenames using this SQL like string")

    parser.add_argument("--tag", action="store", dest="tag", default=None,
                        help="Select files by processing tag")

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
            if df.path == dest:
                logger.warning(f"{df.filename} path already set to destination")
                return
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

    if not (options.tag or options.filepre or options.filelike):
        logger.error("Must specify at least one of --tag or --filepre or --filelike")
        exit()

    session = sessionfactory()
    query = session.query(Header).join(DiskFile).filter(DiskFile.present==True)

    if options.tag:
        query = query.filter(Header.processing_tag==options.tag)

    if options.currentpath is None:
        query = query.filter(DiskFile.path=="")
    else:
        query = query.filter(DiskFile.path==options.currentpath)

    if options.filepre:
        query = query.filter(DiskFile.filename.startswith(options.filepre))

    if options.filelike:
        query = query.filter(DiskFile.filename.like(options.filelike))

    for header in query:
        try:
            moveorlist(header.diskfile, move=options.move, dest=options.dest)
            session.commit()
        except Exception:
            raise

    logger.info("*** s3_rename_processed.py - exiting normally up at "
                f"{datetime.datetime.now()}")
