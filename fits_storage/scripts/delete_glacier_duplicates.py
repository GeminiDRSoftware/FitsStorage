#! /usr/bin/env python3

from sqlalchemy import select
from sqlalchemy.orm import aliased
import datetime

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.config import get_config

from fits_storage.db import sessionfactory
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

    parser.add_argument("--filepre", action="store", dest="filepre",
                        default=None, help="Select files by (case sensitive) "
                                           "filename prefix. Note - filename, "
                                           "not S3 keyname")

    parser.add_argument("--filelike", action="store", dest="filelike",
                        default=None, help="select filenames using this SQL like string")

    parser.add_argument("--path", action="store", dest="path", default=None,
                        help="Look for duplicates with files in path")

    parser.add_argument("--delete", action="store_true", dest="delete",
                        default=False, help="delete keys and update database")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("*** delete_glacier_duplicates.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))


    def deleteorlist(gl, delete=False):
        if delete:
            logger.info(f"Deleting S3 key: {gl.keyname}")
            try:
                if s3.delete_key(gl.keyname) is False:
                    logger.error(f"ClientError deleting key {gl.keyname}")
            except Exception:
                logger.error(f"S3 delete_key {gl.keyname} failed", exc_info=True)
                raise
        else:
            logger.info(f"Keyname {gl.keyname}")

    if fsc.using_s3:
        from fits_storage.server.aws_s3 import Boto3Helper
        if fsc.get('s3_glacier_bucket_name') is None:
            logger.error("No Glacier Bucket name configured. Exiting")
            exit(1)
        s3 = Boto3Helper(fsc.s3_glacier_bucket_name)
    else:
        logger.error("Not an S3 configuration")
        exit()

    # Create two aliases of the table so that we can "join against itself".
    # We select the "root" version as that's the one we want to delete
    glroot = aliased(Glacier)
    glpath = aliased(Glacier)

    stmt = (
        select(glroot).select_from(glroot, glpath)
        .where(glroot.filename == glpath.filename)
        .where(glroot.md5 == glpath.md5)
        .where(glroot.path == '')
        .where(glpath.path != ''))

    if options.filepre:
        stmt = stmt.where(glpath.filename.startswith(options.filepre))

    if options.filelike:
        stmt = stmt.where(glpath.filename.like(options.filelike))

    if options.path:
        stmt = stmt.where(glpath.path == options.path)

    session = sessionfactory()
    for gl in session.execute(stmt).scalars():
        try:
            deleteorlist(gl, delete=options.delete)
            if options.delete:
                session.delete(gl)
                session.commit()
        except Exception:
            raise

    logger.info("*** delete_glacier_duplicates.py - exiting normally up at "
                f"{datetime.datetime.now()}")
