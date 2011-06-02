import sys
sys.path=['/opt/sqlalchemy/lib/python2.5/site-packages', '/astro/iraf/x86_64/gempylocal/lib/stsci_python/lib/python2.5/site-packages']+sys.path

import FitsStorage
import FitsStorageConfig
from FitsStorageLogger import *
from FitsStorageUtils.AddToIngestQueue import *
import os
import re
import datetime
import time

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--filename", action="store", type="string", dest="filename", help="filename of uploaded file to ingest")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
now = datetime.datetime.now()
logger.info("*********  ingest_uploaded_calibration.py - starting up at %s" % now)

# Move the file to it's appropriate loaction in storage_root/path

# Construct the full path names and move the file into place
src = os.path.join(FitsStorageConfig.upload_staging_path, options.filename)
dst = os.path.join(FitsStorageConfig.storage_root, FitsStorageConfig.processed_cals_path, options.filename)
logger.debug("Moving %s to %s" % (src, dst))
os.rename(src, dst)

session = sessionfactory()

logger.info("Queueing for Ingest: %s / %s" % (FitsStorageConfig.processed_cals_path, options.filename))
addto_ingestqueue(session, options.filename, FitsStorageConfig.processed_cals_path)

session.close()
now=datetime.datetime.now()
logger.info("*** add_to_ingestqueue.py exiting normally at %s" % now)

