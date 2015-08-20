import os
from fits_storage.orm import session_scope
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.utils.ingest_standards import ingest_standards
from fits_storage.fits_storage_config import fits_aux_datadir
import datetime

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", type="string", dest="filename", help="Standards text filename")
parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run in background mode")

(options, args) = parser.parse_args()

# Logging level to debug?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********  ingest_standards.py - starting up at %s" % datetime.datetime.now())

with session_scope() as session:
    if options.filename:
        filename = options.filename
    else:
        filename = os.path.join(fits_aux_datadir, "standards.txt")

    logger.info("Reading from file: %s" % filename)

    ingest_standards(session, filename)

logger.info("*** ingest_standards exiting normally at %s" % datetime.datetime.now())
