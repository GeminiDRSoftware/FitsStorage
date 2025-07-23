import datetime
import sys

from fits_storage.config import get_config
from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix

fsc = get_config()
if fsc.using_s3:
    from fits_storage.server.aws_s3 import Boto3Helper
    s3 = Boto3Helper()

if __name__ == "__main__":

    # Option Parsing
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--filepre", action="store", dest="filepre", help="File prefix to operate on, eg N20090130, N200812 etc")
    parser.add_argument("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_argument("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_argument("--delete", action="store_true", dest="delete", help="Actually delete files rather than just count them")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.filepre:
        setlogfilesuffix(options.filepre)
        
    # Announce startup
    logger.info("*********    s3_delete_previews.py - starting up at %s" % datetime.datetime.now())

    if not fsc.using_s3:
        logger.error("This script is only useable on installations using S3 for storage")
        sys.exit(1)

    if options.filepre:
        logger.info("Querying files from S3 bucket by filepre")
        fulllist = s3.key_names_with_prefix(options.filepre)
    else:
        logger.info("Querying files for ingest from S3 bucket")
        fulllist = s3.key_names()

    logger.info(f"Got total of {len(fulllist)} files to consider. Selecting previews...")

    previews = []
    for f in fulllist:
        if f.endswith("fits.bz2_preview.jpg"):
            previews.append(f)
            logger.debug(f"{f} is a preview file")
        else:
            logger.debug(f"{f} is not a preview file")

    if options.delete:
        for key in previews:
            if options.dryrun:
                logger.info(f"Dryrun - not actually deleting {key}")
            else:
                result = s3.delete_key(key)
                if result:
                    logger.info(f"Deleted {key}")
                else:
                    logger.error(f"Failed to delete {key}")
    else:
        logger.info("Found %d preview files", len(previews))

    logger.info("** s3_delete_previews.py exiting normally")
