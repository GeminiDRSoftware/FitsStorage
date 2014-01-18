from orm import sessionfactory
from logger import logger, setdebug
from utils.ingest_standards import ingest_standards
import datetime

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", type="string", dest="filename", default="data/standards.txt", help="Standards text filename")
parser.add_option("--clean", action="store_true", dest="clean", help="Delete all rows in the table before adding")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")

(options, args) = parser.parse_args()

# Logging level to debug?
setdebug(options.debug)

# Annouce startup
logger.info("*********  ingest_standards.py - starting up at %s" % datetime.datetime.now)

session = sessionfactory()

if(options.clean):
    logger.info("Deleting all rows in standards table")
    session.execute("DELETE FROM standards")

ingest_standards(session, options.filename)

session.close()
logger.info("*** ingest_standards exiting normally at %s" % datetime.datetime.now)

