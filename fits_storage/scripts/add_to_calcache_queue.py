from fits_storage.orm import sessionfactory
from fits_storage.orm.calcachequeue import CalCacheQueue
from fits_storage.orm.header import Header
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon
import datetime
import sys

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file-re", action="store", type="string", dest="file_pre", help="filename prefix to select files to queue by")
parser.add_option("--lastdays", action="store", type="int", dest="lastdays", help="queue observations with ut_datetime in last n days")
parser.add_option("--all", action="store_true", dest="all", help="queue all observations in database. Use with Caution")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    add_to_calcache_queue.py - starting up at %s" % datetime.datetime.now())

if (not options.file_pre) and (not options.lastdays) and (not options.all):
    logger.error("You must give either a file_pre or lastdays, or use the all flag")
    sys.exit(1)
    
session = sessionfactory()
try:
    # Get a list of header IDs to queue. NB files don't have to be present, but we do want the canonical one
    # We use the header.ut_datetime as the sortkey for the queue
    query = session.query(Header.id, Header.ut_datetime).select_from(Header, DiskFile).filter(DiskFile.id == Header.diskfile_id)
    query = query.filter(DiskFile.canonical == True)

    if options.file_pre:
        query = query.filter(DiskFile.filename.like(options.file_pre+'%'))

    if options.lastdays:
        now = datetime.datetime.utcnow()
        delta = datetime.timedelta(days=options.lastdays)
        then = now - delta
        query = query.filter(Header.ut_datetime > then)

    hids = query.all()

    logger.info("Got %d header items to queue" % len(hids))
    
    num = 0
    for hid in hids:
        logger.debug("Adding CalCacheQueue with obs_hid %s", hid[0])
        cq = CalCacheQueue(hid[0], sortkey=hid[1])
        session.add(cq)
        num += 1
        if num % 1000 == 0:
            logger.info("Committing batch %d", num)
            session.commit()

    session.commit()

finally:
    session.close()

logger.info("*** add_to_calcache_queue.py exiting normally at %s" % datetime.datetime.now())

