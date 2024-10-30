#! /usr/bin/env python3

import sys
import datetime

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.queues.queue.exportqueue import ExportQueue
from fits_storage.db.list_headers import list_headers
from fits_storage.db.selection.get_selection import from_url_things
from fits_storage.db import session_scope

"""
Script to add files to the queue for export to another server (eg the 
archive or tape-server etc). The files must already be ingested into the 
database on the local (sending) server.

This script simply adds files to the export queue.  The export service
(service_export_queue.py) actually does the export.
"""

if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--selection", action="store", type="string",
                      dest="selection", help="file selection to use")
    parser.add_option("--destination", action="store", type="string",
                      dest="destination", help="server name to export to")
    parser.add_option("--debug", action="store_true", dest="debug",
                      help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("***   add_to_export_queue.py - starting up at %s"
                % datetime.datetime.now())
    # This script doesn't actually use any config file items.
    # logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    if not options.selection:
        logger.error("You must specify a file selection")
        sys.exit(1)

    if not options.destination:
        logger.error("You must specify a destination")
        sys.exit(1)

    # Files must be present on disk in order to be exported
    selection = options.selection + '/present'

    order = []
    things = selection.split('/')
    selection = from_url_things(things)
    logger.info("Selection: %s" % selection)
    if selection.openquery:
        logger.warning("Selection is open - this may not be what you want")

    with session_scope() as session:
        logger.info("Getting header object list")
        headers = list_headers(selection, order, session=session, unlimit=True)

        # Looping through the header list directly for the add
        # is really slow if the list is big.
        logger.info("Building filename and path lists")
        filenames = []
        paths = []
        for header in headers:
            filenames.append(header.diskfile.filename)
            paths.append(header.diskfile.path)

        headers = None

        eq = ExportQueue(session, logger=logger)
        n = len(filenames)
        for i, (filename, path) in enumerate(zip(filenames, paths), 1):
            logger.info("Queueing for Export: (%d/%d): %s" % (i, n, filename))
            eq.add(filename, path, options.destination)

    logger.info("*** add_to_exportqueue.py exiting normally at %s"
                % datetime.datetime.now())
