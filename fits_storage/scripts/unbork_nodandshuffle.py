from fits_storage.orm import session_scope

from fits_storage.logger import logger, setdebug

from sqlalchemy import join
import datetime
from optparse import OptionParser

from fits_storage.orm.fulltextheader import FullTextHeader
from fits_storage.orm.header import Header

parser = OptionParser()
parser.add_option("--start", action="store", type="int", help="specify a start id")
parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of headers to examine")
parser.add_option("--step", action="store", type="int", help="specify a limit on the number of headers to do per batch")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Don't actually fix")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


# Annouce startup
logger.info("*********    unbork_nodandshuffle.py - starting up at %s" % datetime.datetime.now())

# Get a database session
start = options.start
end = start + options.limit
while start <= end:
    stop = min(end, start+options.step)
    with session_scope() as session:
        # Get a list of all diskfile_ids marked as present
        query = session.query(Header, FullTextHeader) \
            .filter(Header.id >= start) \
            .filter(Header.id < stop) \
            .filter(FullTextHeader.diskfile_id == Header.diskfile_id) \
            .filter(Header.instrument.like('GMOS%')) \
            .filter(Header.detector_readmode_setting == 'Classic').all()

        logger.info("Starting checking...")

        count = 0
        for hdr, fth in query:
            if "'NODANDSHUFFLE'" in fth.fulltext:
                if options.dryrun:
                    logger.info("Would update header: %s" % hdr.id)
                else:
                    hdr.detector_readmode_setting = 'NodAndShuffle'
                    count = count + 1
                    if (count % 1000) == 0:
                        session.commit()
                        count = 0
        if count > 0:
            session.commit()
    start = start + options.step

logger.info("*** unbork_nodandshuffle.py exiting normally at %s" % datetime.datetime.now())
