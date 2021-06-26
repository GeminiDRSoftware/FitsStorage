import datetime
import sys

from sqlalchemy import join, desc

from gemini_obs_db import session_scope
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from fits_storage.fits_storage_config import using_s3
from fits_storage.utils.aws_s3 import get_helper
from fits_storage.logger import logger, setdebug, setdemon


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file-pre", action="store", type="string", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
    parser.add_option("--yesimsure", action="store_true", dest="yesimsure", default=False, help="Needed for sanity check")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    s3_unconditional_delete_files.py - starting up at %s" % datetime.datetime.now())

    if not using_s3:
        logger.error("This script is only useable on installations using S3 for storage")
        sys.exit(1)

    if not options.yesimsure:
        logger.info("This is a really dangerous script to run. If you're not sure, don't do this.")
        logger.info("This will unconditionally delete files from the S3 storage")
        logger.error("You need to say --yesimsure to make it work")
        sys.exit(2)

    if not options.filepre or len(options.filepre) < 5:
        logger.error("filepre is dangerously short, please re-think what youre doing")
        sys.exit(3)

    with session_scope() as session:
        query = (
            session.query(DiskFile).select_from(join(File, DiskFile))
                    .filter(DiskFile.present==True)
                    .filter(File.name.like("{}%".format(options.filepre)))
                )

        nfiles = query.count()

        if nfiles == 0:
            logger.info("No Files found matching file-pre. Exiting")
            sys.exit(0)

        logger.info("Got %d files for deletion" % nfiles)

        s3 = get_helper()

        for diskfile in query:
            logger.info("Deleting file %s" % diskfile.filename)
            key = s3.get_key(diskfile.filename)
            if key is None:
                logger.error("File %s did not exist on S3 anyway!" % diskfile.filename)
            else:
                key.delete()

            diskfile.present = False
            session.commit()

    logger.info("** s3_unconditional_delete_files.py exiting normally")
