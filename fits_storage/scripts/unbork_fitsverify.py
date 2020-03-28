from fits_storage.orm import session_scope
from fits_storage.orm.file import File
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport

from fits_storage.logger import logger, setdebug, setdemon

from sqlalchemy import join
import datetime
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of files to examine. The list is sorted by lastmod time before the limit is applied")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Don't actually fix")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)


# Annouce startup
logger.info("*********    unbork_fitsverify.py - starting up at %s" % datetime.datetime.now())

# Get a database session
with session_scope() as session:
    # Get a list of all diskfile_ids marked as present
    query = session.query(DiskFileReport)

    # Did we get a limit option?
    if(options.limit):
        query = query.limit(options.limit)

    logger.info("evaluating number of rows...")
    n = query.count()
    logger.info("%d reports to check" % n)

    logger.info("Starting checking...")

    count = 0
    for dfr in query:
        fv = dfr.fvreport
        if isinstance(fv, str):
            logger.debug("Found string for report id %s, ignoring" % dfr.id)
            if fv.startswith('\\x'):
                logger.info("Found stringified bytes fv report value for report id %s, fixing" % dfr.id)
                fv = bytes.fromhex(fv[2:].replace('\\x', '')).decode('utf-8')
                if 'fitsverify' not in fv:
                    logger.info("Unable to fix fitsverify report for report id %s" % dfr.id)
                else:
                    logger.debug("Converted bad string")
                    if options.dryrun:
                        logger.info("Would save: %s" % fv)
                    else:
                        dfr.fvreport = fv
                        count = count + 1
                        if (count % 1000) == 0:
                            session.commit()
                            count = 0
        else:
            logger.info("Found bad fv report for report id %s, fixing" % dfr.id)
            fvr_bytes = fv
            fv = fvr_bytes.encode('utf-8', errors='ignore')
            if options.dryrun:
                logger.info("Would save: %s" % fv)
            else:
                dfr.fvreport = fv
                session.save(dfr)
                count = count+1
                if (count % 1000) == 0:
                    session.commit()
                    count = 0
    if count > 0:
        session.commit()

logger.info("*** unbork_fitsverify.py exiting normally at %s" % datetime.datetime.now())
