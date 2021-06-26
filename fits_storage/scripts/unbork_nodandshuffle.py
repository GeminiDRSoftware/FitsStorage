from gemini_obs_db import session_scope

from fits_storage.logger import logger, setdebug

from sqlalchemy import join
import datetime
from optparse import OptionParser

from fits_storage.orm.fulltextheader import FullTextHeader
from gemini_obs_db.header import Header


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of headers to examine")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)


    # Annouce startup
    logger.info("*********    unbork_nodandshuffle.py - starting up at %s" % datetime.datetime.now())

    # Get a database session
    done = False
    while not done:
        done = True
        with session_scope() as session:
            # Get a list of all diskfile_ids marked as present
            query = session.query(Header, FullTextHeader) \
                .filter(FullTextHeader.diskfile_id == Header.diskfile_id) \
                .filter(Header.instrument.like('GMOS%')) \
                .filter(Header.detector_readmode_setting == 'BORKED').limit(options.limit)

            logger.info("Starting checking...")

            for hdr, fth in query:
                # if we got any matches, we'll want to repeat our search
                done = False
                if "'NODANDSHUFFLE'" in fth.fulltext:  # always true, we could do something clever to validate it's the tags
                    readmode = 'NodAndShuffle'
                else:
                    readmode = 'Classic'
                hdr.detector_readmode_setting = readmode
            if not done:
                session.commit()

    logger.info("*** unbork_nodandshuffle.py exiting normally at %s" % datetime.datetime.now())
