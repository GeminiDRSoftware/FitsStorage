from fits_storage.utils.targetqueue import TargetQueueUtil
from fits_storage.logger import logger, setdebug, setdemon
from gemini_obs_db.db import session_scope
from gemini_obs_db.orm.diskfile import DiskFile
from gemini_obs_db.orm.header import Header
import datetime
import sys

"""
Script to add files in the FITS Server to the queue for finding moving targets.

This script will add files in the system to the queue for checking for known targets.
"""
if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file-pre", action="store", type="string", dest="file_pre", help="filename iprefix to select files to queue by")
    parser.add_option("--instrument", action="store", type="string", dest="instrument", help="add files from this instrument only")
    parser.add_option("--all", action="store_true", dest="all", help="queue all observations in database. Use with Caution")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    add_to_target_queue.py - starting up at %s" % datetime.datetime.now())

    if not (options.file_pre or options.all):
        logger.error("You must give either a file_pre, or use the all flag")
        sys.exit(1)

    with session_scope() as session:
        # Get a list of diskfile IDs to queue.
        query = session.query(DiskFile)

        if options.instrument:
            query = query.select_from(DiskFile, Header).filter(Header.diskfile_id == DiskFile.id)
            query = query.filter(Header.instrument == options.instrument)

        query = query.filter(DiskFile.canonical == True)

        if options.file_pre:
            query = query.filter(DiskFile.filename.like(options.file_pre+'%'))

        dfs = query.all()

        logger.info("Got %d diskfileitems to queue" % len(dfs))

        TargetQueueUtil(session, logger).process(dfs)

    logger.info("*** add_to_target_queue.py exiting normally at %s" % datetime.datetime.now())