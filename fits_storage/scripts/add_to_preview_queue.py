#! /usr/bin/env python3

import datetime
import sys

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-pre", action="store", type="string", dest="file_pre",
                  help="filename prefix to select files to queue by")
parser.add_option("--instrument", action="store", type="string",
                  dest="instrument", help="add files from this instrument only")
parser.add_option("--all", action="store_true", dest="all",
                  help="queue all files in database. Use with Caution")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
parser.add_option("--force", action="store_true", dest="force", default=False,
                  help="Force (re)creation of the previews even if they "
                       "already exist")
parser.add_option("--scavengeonly", action="store_true", dest="scavengeonly",
                  default=False, help="Do not create new previews, only "
                                      "scavenge existing preview files")

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Announce startup
logger.info("***   add_to_preview_queue.py - starting up at %s",
            datetime.datetime.now())

if not (options.file_pre or options.all):
    logger.error("You must give either a file_pre, or use the all flag")
    sys.exit(1)

with session_scope() as session:
    # Get a list of diskfile IDs to queue.
    query = session.query(DiskFile)

    if options.instrument:
        query = query.select_from(DiskFile, Header).\
            filter(Header.diskfile_id == DiskFile.id)\
            .filter(Header.instrument == options.instrument)

    query = query.filter(DiskFile.canonical == True)

    if options.file_pre:
        query = query.filter(DiskFile.filename.startswith(options.file_pre))
    dfs = query.all()

    num = len(dfs)
    logger.info("Got %d diskfiles to queue", num)

    i = 0
    for df in dfs:
        pqe = PreviewQueueEntry(df, force=options.force,
                                scavengeonly=options.scavengeonly)
        session.add(pqe)
        session.commit()
        i += 1
        logger.debug("Adding %s to preview queue (%s/%s)", df.filename, i, num)

    logger.info("Added %s entries to preview queue", i)

logger.info("*** add_to_preview_queue.py exiting normally at %s",
            datetime.datetime.now())
