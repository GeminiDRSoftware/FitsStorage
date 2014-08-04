from orm import sessionfactory
from fits_storage_config import storage_root, using_s3
from logger import logger, setdebug, setdemon
from utils.exportqueue import add_to_exportqueue
from web.list_headers import list_headers
from web.selection import getselection, openquery
import os
import sys
import re
import datetime
import time
if (using_s3):
    from fits_storage_config import s3_bucket_name, aws_access_key, aws_secret_key
    from boto.s3.connection import S3Connection

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--selection", action="store", type="string", dest="selection", help="file selection to use")
parser.add_option("--destination", action="store", type="string", dest="destination", help="server name to export to")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

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

session = sessionfactory()

selection = options.selection + '/present'

orderby = []
things = selection.split('/')
selection = getselection(things)
logger.info("Selection: %s" % selection)
logger.info("Selection is open: %s" % openquery(selection))

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

i = 0
n = len(filenames)
for i in range(n):
    logger.info("Queueing for Export: (%d/%d): %s" % (i, n, filenames[i]))
    add_to_exportqueue(session, filenames[i], paths[i], destination)

session.close()
logger.info("*** add_to_exportqueue.py exiting normally at %s" % datetime.datetime.now())

