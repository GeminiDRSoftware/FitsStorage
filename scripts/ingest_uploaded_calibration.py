import os
import datetime

from orm import sessionfactory
from fits_storage_config import storage_root, upload_staging_path, processed_cals_path
from logger import logger, setdemon, setdebug
from utils.add_to_ingestqueue import addto_ingestqueue

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
src = os.path.join(upload_staging_path, options.filename)
dst = os.path.join(storage_root, processed_cals_path, options.filename)
logger.debug("Moving %s to %s" % (src, dst))
# We can't use os.rename as that keeps the old permissions and ownership, which we specifically want to avoid
fin = open(src, 'r')
fout = open(dst, 'w')
# this is a bit brute force
buf = fin.read()
fout.write(buf)
buf = None
fin.close()
fout.close()
os.remove(src)

session = sessionfactory()

logger.info("Queueing for Ingest: %s / %s" % (processed_cals_path, options.filename))
addto_ingestqueue(session, options.filename, processed_cals_path)

session.close()
now = datetime.datetime.now()
logger.info("*** add_to_ingestqueue.py exiting normally at %s" % now)

