from fits_storage.orm import session_scope
from fits_storage.orm.file import File
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.diskfilereport import DiskFileReport

from fits_storage.logger import logger, setdebug, setdemon

from sqlalchemy import join
import datetime
from optparse import OptionParser


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--start", action="store", type="int", help="specify a start id")
    parser.add_option("--limit", action="store", type="int", help="specify a limit on the number of reports to examine")
    parser.add_option("--step", action="store", type="int", help="specify a limit on the number of reports to do per batch")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Don't actually fix")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)


    # Annouce startup
    logger.info("*********    unbork_fitsverify.py - starting up at %s" % datetime.datetime.now())

    # Get a database session
    start = options.start
    end = start + options.limit
    while start <= end:
        stop = min(end, start+options.step)
        with session_scope() as session:
            # Get a list of all diskfile_ids marked as present
            query = session.query(DiskFileReport).filter(DiskFileReport.id >= start) \
                .filter(DiskFileReport.id < stop)

            # Did we get a limit option?
            # if(options.limit):
            #     query = query.limit(options.limit)

            # logger.info("evaluating number of rows...")
            # n = query.count()
            # logger.info("%d reports to check" % n)

            logger.info("Starting checking...")

            count = 0
            for dfr in query:
                fv = dfr.fvreport
                if isinstance(fv, str):
                    if fv.startswith('\\x'):
                        fv = bytes.fromhex(fv[2:].replace('\\x', '')).decode('utf-8')
                        if 'fitsverify' not in fv:
                            logger.info("Unable to fix fitsverify report for report id %s" % dfr.id)
                        else:
                            if options.dryrun:
                                logger.info("Would save: %s" % fv)
                            else:
                                dfr.fvreport = fv
                                count = count + 1
                                if (count % 1000) == 0:
                                    session.commit()
                                    count = 0
                else:
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
        start = start + options.step

    logger.info("*** unbork_fitsverify.py exiting normally at %s" % datetime.datetime.now())
