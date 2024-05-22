#! /usr/bin/env python3

import datetime
import sys

from sqlalchemy.exc import IntegrityError

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

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-pre", action="store", type="string", dest="file_pre",
                  help="filename prefix to select files to queue by")
parser.add_option("--lastdays", action="store", type="int", dest="lastdays",
                  help="queue observations with ut_datetime in last n days")
parser.add_option("--instrument", action="store", dest="instrument",
                  type="string", help="Only add files for this instrument")
parser.add_option("--all", action="store_true", dest="all",
                  help="queue all observations in database. Use with Caution")
parser.add_option("--ignore-mdbad", action="store_true", dest="ignore_mdbad",
                  help="don't add files if they fail metadata validation")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
parser.add_option("--bulk-add", action="store_true", dest="bulk_add",
                  help="Add all the entries in one database commit. This is a "
                       "lot faster for a large number of entries, but any one "
                       "of them has an error, they will all fail to add")
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

    if not options.ignore_mdbad:
        query = query.filter(DiskFile.mdready == True)

    if options.file_pre:
        query = query.filter(DiskFile.filename.like(options.file_pre+'%'))

    if options.lastdays:
        then = datetime.datetime.utcnow() - \
                datetime.timedelta(days=options.lastdays)
        query = query.filter(Header.ut_datetime > then)

    headers = query.all()

    logger.info("Got %d header items to queue" % len(headers))

    # Looping through the header list directly for the add is really slow
    # if the list is big..

    logger.info("Building (hid, filename) list...")
    items = []
    for header in headers:
        items.append((header.id, header.diskfile.filename))

    # Note, we don't try and batch these commits as if there's an
    # IntegrityError resulting from an entry already existing, that will
    # fail the entire commit and thus the entire batch.
    i = 0
    n = len(items)
    for (hid, filename) in items:
        i += 1
        logger.info("Creating CalCacheQueueEntry with obs_hid %s, "
                    "filename %s (%d/%d)", hid, filename, i, n)
        cqe = CalCacheQueueEntry(hid, filename)
        try:
            logger.debug("Adding CalCacheQueueEntry")
            session.add(cqe)
            if not options.bulk_add:
                session.commit()
        except IntegrityError:
            session.rollback()
            logger.debug("IntegrityError adding hid %s - filename %s"
                         "to queue. Likely they are already on the"
                         "queue", hid, filename)
    if options.bulk_add:
        session.commit()

logger.info("*** add_to_calcache_queue.py exiting normally at %s" %
            datetime.datetime.now())
