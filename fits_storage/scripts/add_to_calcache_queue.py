#! /usr/bin/env python3

import datetime
import sys

from sqlalchemy.exc import IntegrityError

from fits_storage.queues.queue import CalCacheQueue
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.db import session_scope
from fits_storage import utcnow


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-pre", action="store", type="string", dest="file_pre",
                  help="filename prefix to select files to queue by")
parser.add_option("--lastdays", action="store", type="int", dest="lastdays",
                  help="queue observations with ut_datetime in last n days")
parser.add_option("--instrument", action="store", dest="instrument",
                  type="string", help="Only add files for this instrument")
parser.add_option("--include-eng", action="store", dest="include_eng",
                  default=False, help="Include engineering files")
parser.add_option("--all", action="store_true", dest="all",
                  help="queue all observations in database. Use with Caution")
parser.add_option("--ignore-mdbad", action="store_true", dest="ignore_mdbad",
                  help="don't add files if they fail metadata validation")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
parser.add_option("--no-bulk-add", action="store_true", dest="no_bulk_add",
                  help="Add the entries in individual database commits. This is"
                       " a lot slower for a large number of entries, but avoids"
                       " the problem with a bulk add where if any one entry has"
                       " an error, they will all fail to add")
parser.add_option("--no-precheck", action="store_true", dest="noprecheck",
                  help="Do not exclude header IDs already on the queue from the"
                       " initial header list. We do this by default so that we "
                       "can reasonably safely do a bulk commit, otherwise this "
                       "is really slow for large numbers of entries.")
options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   add_to_calcache_queue.py - starting up at %s"
            % datetime.datetime.now())

if not (options.file_pre or options.lastdays or options.all):
    logger.error("You must give either a file-pre or lastdays, "
                 "or use the all flag")
    sys.exit(1)

with session_scope() as session:
    # Get a list of header IDs to queue. NB files don't have to be
    # present, but we do want the canonical one.
    # We use the header.ut_datetime as the sortkey for the queue
    query = session.query(Header).join(DiskFile)\
        .filter(DiskFile.canonical == True)

    if not options.noprecheck:
        subquery = session.query(CalCacheQueueEntry.obs_hid).\
            filter(CalCacheQueueEntry.inprogress == False).\
            filter(CalCacheQueueEntry.fail_dt !=
                   CalCacheQueueEntry.fail_dt_false)

        query = query.filter(Header.id.not_in(subquery))

    if not options.ignore_mdbad:
        query = query.filter(DiskFile.mdready == True)

    if options.file_pre:
        query = query.filter(DiskFile.filename.like(options.file_pre+'%'))

    if options.lastdays:
        then = utcnow() - datetime.timedelta(days=options.lastdays)
        query = query.filter(Header.ut_datetime > then)

    if options.instrument:
        query = query.filter(Header.instrument == options.instrument)

    if options.include_eng:
        pass
    else:
        query = query.filter(Header.engineering == False)

    headers = query.all()

    logger.info("Got %d header items to queue" % len(headers))

    # Looping through the header list directly for the add is really slow
    # if the list is big.

    logger.info("Building (hid, filename) list...")
    items = []
    for header in headers:
        items.append((header.id, header.diskfile.filename))


    ccq = CalCacheQueue(session, logger=logger)
    individual_commit = True if options.no_bulk_add else False
    i = 0
    n = len(items)
    for (hid, filename) in items:
        i += 1
        logger.info("Adding hid %d - filename %s to CalCache queue (%d/%d)",
                    hid, filename, i, n)
        ccq.add(hid, filename, commit=individual_commit)
    if not individual_commit:
        try:
            logger.info("Committing bulk-add.")
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.debug("Bulk add commit failed. None of the items have been "
                         "added. Suggest re-run without bulk-add, or ensure "
                         "queue is empty before adding.")


logger.info("*** add_to_calcache_queue.py exiting normally at %s" %
            datetime.datetime.now())
