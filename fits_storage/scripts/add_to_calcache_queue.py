import datetime
import sys

from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.db import session_scope

"""
Script to add files to ingest into the FITS Server

This script will add files in the system to the queue for ingest.
The ingest service will then examine the files and create 
`fits_storage.orm.DiskFile` and `fits_storage.orm.Header` records.
"""
if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file-re", action="store", type="string",
                      dest="file_pre",
                      help="filename prefix to select files to queue by")
    parser.add_option("--lastdays", action="store", type="int",
                      dest="lastdays",
                      help="queue observations with ut_datetime in last n days")
    parser.add_option("--all", action="store_true",
                      dest="all",
                      help="queue all observations in database. "
                           "Use with Caution")
    parser.add_option("--ignore_mdbad", action="store_true",
                      dest="ignore_mdbad",
                      help="don't add files if they fail metadata validation")
    parser.add_option("--debug", action="store_true",
                      dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    add_to_calcache_queue.py - starting up at %s"
                % datetime.datetime.now())

    if not (options.file_pre or options.lastdays or options.all):
        logger.error("You must give either a file_pre or lastdays, "
                     "or use the all flag")
        sys.exit(1)

    with session_scope() as session:
        # Get a list of header IDs to queue. NB files don't have to be
        # present, but we do want the canonical one.
        # We use the header.ut_datetime as the sortkey for the queue
        query = session.query(Header.id, DiskFile.filename)\
            .select_from(Header, DiskFile)\
            .filter(DiskFile.id == Header.diskfile_id)\
            .filter(DiskFile.canonical is True)

        if not options.ignore_mdbad:
            query = query.filter(DiskFile.mdready is True)

        if options.file_pre:
            query = query.filter(DiskFile.filename.like(options.file_pre+'%'))

        if options.lastdays:
            then = datetime.datetime.utcnow() - \
                   datetime.timedelta(days=options.lastdays)
            query = query.filter(Header.ut_datetime > then)

        hids = query.all()

        logger.info("Got %d header items to queue" % len(hids))

        for num, (hid, filename) in enumerate(hids):
            logger.debug("Adding CalCacheQueue with obs_hid %s", hid)
            cqe = CalCacheQueueEntry(hid, filename)
            session.add(cqe)
            if num % 1000 == 0:
                logger.info("Committing batch %d", num)
                session.commit()

    logger.info("*** add_to_calcache_queue.py exiting normally at %s" %
                datetime.datetime.now())
