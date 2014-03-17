from orm import sessionfactory
from fits_storage_config import storage_root, using_s3
from logger import logger, setdebug, setdemon
from utils.add_to_exportqueue import addto_exportqueue
from web.summary import list_headers
from web.selection import getselection
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

if(not options.selection):
    logger.error("You must specify a file selection")
    sys.exit(1)

if(not options.destination):
    logger.error("You must specify a destination")
    sys.exit(1)

session = sessionfactory()

selection = options.selection + '/present'

orderby = []
things = selection.split('/')
selection = getselection(things)
logger.info("Selection: %s" % selection)

headers = list_headers(session, selection, orderby)

i = 0
n = len(headers)
for header in headers:
    i += 1
    logger.info("Queueing for Export: (%d/%d): %s" % (i, n, header.diskfile.filename))
    addto_exportqueue(session, header.diskfile.filename, header.diskfile.path, options.destination)

session.close()
logger.info("*** add_to_exportqueue.py exiting normally at %s" % datetime.datetime.now())

