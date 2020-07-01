import sys
import os
import traceback
import datetime
import time
import shutil
from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingestqueue import IngestQueueUtil
from fits_storage.fits_storage_config import using_s3, storage_root, dhs_perm

# Utility functions
def check_present(session, filename):
    df = session.query(DiskFile).filter(DiskFile.filename==filename).filter(DiskFile.present==True).first()
    return True if df else False

def copy_over(session, iq, logger, filename, dryrun):
    src = os.path.join(dhs_perm, filename)
    dest = os.path.join(storage_root, filename)
    # If the Destination file already exists, skip it
    if os.access(dest, os.F_OK | os.R_OK):
        logger.info("%s already exists on storage_root - skipping", filename)
        return True
    # If the source path is a directory, skip is
    if os.path.isdir(src):
        logger.info("%s is a directory - skipping", filename)
        return True
    # If one of these wierd things, skip it
    if filename in ['.bplusvtoc_internal', '.vtoc_internal']:
        logger.info("%s is a wierd thing - skipping", filename)
        return True
    # If lastmod time is within 5 secs, DHS may still be writing it. Skip it
    lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(src))
    age = datetime.datetime.now() - lastmod
    age = age.total_seconds()
    if age < 5:
        logger.debug("%s is too new (%.1f)- skipping this time round", filename, age)
        return False
    else:
        logger.debug("%s age is OK: %.1f seconds", filename, age)
    # OK, try and copy the file over.
    try:
        if dryrun:
            logger.info("Dryrun - not actually copying %s", filename)
        else:
            logger.info("Copying %s to %s", filename, storage_root)
            # We can't use shutil.copy, because it preserves mode of the
            # source file, making the umask totally useless. Under the hood,
            # copy is just a shutil.copyfile + shutil.copymode. We'll
            # use copyfile instead.
            dst = os.path.join(storage_root, os.path.basename(src))
            shutil.copyfile(src, dst)
            logger.info("Adding %s to IngestQueue", filename)
            iq.add_to_queue(filename, '', force=False, force_md5=False, after=None)
    except:
        logger.error("Problem copying %s to %s", src, storage_root)
        logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1],
                                                 traceback.format_tb(sys.exc_info()[2]))
        return False
    # Add it to the ingest queue here
    return True


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False, help="Don't actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run in background mode")

    (options, args) = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********  copy_from_dhs.py - starting up at %s" % datetime.datetime.now())

    if using_s3:
        logger.info("This should not be used with S3 storage. Exiting")
        sys.exit(1)

    logger.info("Doing Initial DHS directory scan...")
    # Get initial DHS directory listing
    dhs_list = set(os.listdir(dhs_perm))
    logger.info("... found %d files", len(dhs_list))
    known_list = set()

    with session_scope() as session:
         logger.debug("Instantiating IngestQueueUtil object")
         iq = IngestQueueUtil(session, logger)
         logger.info("Starting looping...")
         while True:
             todo_list = dhs_list - known_list
             logger.info("%d new files to check", len(todo_list))
             for filename in todo_list:
                 if 'tmp' in filename:
                     logger.info("Ignoring tmp file: %s", filename)
                     continue
                 filename = os.path.split(filename)[1]
                 if check_present(session, filename):
                     logger.debug("%s is already present in database", filename)
                     known_list.add(filename)
                 else:
                     if copy_over(session, iq, logger, filename, options.dryrun):
                         known_list.add(filename)
             logger.debug("Pass complete, sleeping")
             time.sleep(5)
             logger.debug("Re-scanning")
             dhs_list = set(os.listdir(dhs_perm))

