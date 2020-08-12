import datetime
from optparse import OptionParser
from sqlalchemy import func, join

from fits_storage.orm import session_scope
from fits_storage.orm.header import Header
from fits_storage.orm.diskfile import DiskFile
from fits_storage.logger import logger, setdebug, setdemon


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)


    # Annouce startup
    logger.info("*********    data_rates.py - starting up at %s" % datetime.datetime.now())

    with session_scope() as session, open("/data/logs/data_rates.py", "w") as f:
        ndays = 1000

        today = datetime.datetime.utcnow().date()
        zerohour = datetime.time(0, 0, 0)
        ddelta = datetime.timedelta(days=1)

        start = datetime.datetime.combine(today, zerohour)
        end = start + ddelta

        for i in range(1, ndays):
            query = (session.query(func.sum(DiskFile.data_size))
                            .select_from(join(Header, DiskFile))
                            .filter(DiskFile.present==True)
                            .filter(Header.ut_datetime.between(start, end)))
            # If any DiskFile.data_size is NULL, the result will be None. The 'or 0' takes care of that
            nbytes = query.one()[0] or 0
            f.write("%s, %f\n" % (str(start.date()), nbytes/1.0E9))
            end = start
            start -= ddelta

    logger.info("*** data_rates.py exiting normally at %s" % datetime.datetime.now())
