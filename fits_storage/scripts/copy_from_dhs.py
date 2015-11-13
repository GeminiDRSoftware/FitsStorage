import sys
import os
import traceback
import datetime
from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.fits_storage_config import using_s3, storage_root, dhs_perm

# Utility functions
def check_present(session, filename):
    df = session.query(DiskFile).filter(DiskFile.filename==filename).filter(DiskFile.present==True).first()
    return True if df else False

def copy_over(session, logger, filename):
    logger.info("Copying over %s", filename)
    src = os.path.join(dhs_perm, filename)
    try:
        shutil.copy(src, storage_root)
    except:
        logger.error("Problem copying %s to %s", src, storage_root)
        logger.error("Exception: %s : %s... %s", sys.exc_info()[0], sys.exc_info()[1],
                                                 traceback.format_tb(sys.exc_info()[2]))
        return False
    # Add it to the ingest queue here
    return True


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
     logger.info("Starting looping...")
     while True:
         todo_list = dhs_list - known_list
         logger.info("%d new files to check", len(todo_list))
         for filename in todo_list:
             filename = os.path.split(filename)[1]
             if check_present(session, filename):
                 logger.debug("%s is already present in database", filename)
             else:
                 if options.dryrun:
                     logger.info("Dryrun - would copy file %s", filename)
                 else:
                     copy_over(session, logger, filename)
             known_list.add(filename)
         logger.debug("Pass complete, sleeping")
         time.sleep(5)
         logger.debug("Re-scanning")
         dhs_list = set(os.listdir(dhs_perm))

