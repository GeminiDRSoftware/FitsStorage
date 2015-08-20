from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.exportqueue import ExportQueueUtil
from fits_storage.web.list_headers import list_headers
from fits_storage.web.selection import getselection, openquery
import os
import sys
import re
import datetime
import time

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--selection", action="store", type="string", dest="selection", help="file selection to use")
parser.add_option("--destination", action="store", type="string", dest="destination", help="server name to export to")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

options, args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    add_to_export_queue.py - starting up at %s" % datetime.datetime.now())

if not options.selection:
    logger.error("You must specify a file selection")
    sys.exit(1)

if not options.destination:
    logger.error("You must specify a destination")
    sys.exit(1)
else:
    destination = options.destination

selection = options.selection + '/present'

orderby = []
things = selection.split('/')
selection = getselection(things)
logger.info("Selection: %s" % selection)
logger.info("Selection is open: %s" % openquery(selection))

with session_scope() as session:
    logger.info("Getting header object list")
    headers = list_headers(session, selection, orderby)

    # For some reason, looping through the header list directly for the add
    # is really slow if the list is big.
    logger.info("Building filename and path lists")
    filenames = []
    paths = []
    for header in headers:
        filenames.append(header.diskfile.filename)
        paths.append(header.diskfile.path)

    headers = None

    export_queue = ExportQueueUtil(session, logger)
    n = len(filenames)
    for i, (filename, path) in enumerate(zip(filenames, paths), 1):
        logger.info("Queueing for Export: (%d/%d): %s" % (i, n, filename))
        export_queue.add_to_queue(filename, path, destination)

logger.info("*** add_to_exportqueue.py exiting normally at %s" % datetime.datetime.now())
