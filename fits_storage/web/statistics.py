"""
This is a script which defines functions for generating content, general, and possibly usage statistics on the database. It queries the database for stats and outputs them as HTML
"""

from sqlalchemy import desc, func, join, and_, or_, cast, between, distinct
from sqlalchemy import Interval, Date, String
from sqlalchemy.sql import column
from sqlalchemy.orm import aliased

from ..fits_storage_config import fits_system_status
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.header import Header
from ..orm.ingestqueue import IngestQueue

from ..utils.query_utils import to_int, null_to_zero
from ..utils.web import get_context

from . import templating

from datetime import datetime, timedelta, time as dt_time, date as dt_date
from collections import defaultdict, namedtuple

@templating.templated("statistics/stats.html")
def stats():
    """
    Provides live statistics on fits database: total filesize, ingest queue status, and datarate for various date ranges is queried. Information is
    presented in html in the browser in a list format.
    """

    session = get_context().session
    # DiskFile table statistics
    DiskFileStats = namedtuple('DiskFileStats', "total_rows present_rows present_size latest last_minute last_hour last_day last_queries")
    df_query = session.query(DiskFile)
    def number_of_entries_within(delta):
        return df_query.filter(DiskFile.entrytime > (datetime.now() - delta)).count()

    diskfile_stats = DiskFileStats(
        total_rows   = df_query.count(),
        present_rows = df_query.filter(DiskFile.present == True).count(),
        present_size = session.query(func.sum(DiskFile.file_size)).filter(DiskFile.present == True).one()[0],
        latest       = session.query(func.max(DiskFile.entrytime)).one()[0],
        last_minute  = number_of_entries_within(timedelta(minutes=1)),
        last_hour    = number_of_entries_within(timedelta(hours=1)),
        last_day     = number_of_entries_within(timedelta(days=1)),
        last_queries = df_query.order_by(desc(DiskFile.entrytime)).limit(10)
        )

    # Ingest queue stats
    IngestStats = namedtuple('IngestStats', "count in_progress")
    ingest_stats = IngestStats(
        count = session.query(IngestQueue).count(),
        in_progress = session.query(IngestQueue).filter(IngestQueue.inprogress == True).count(),
        )

    # Data rate statistics
    today = datetime.utcnow().date()
    zerohour = dt_time(0, 0, 0)
    comb = datetime.combine(today, zerohour)
    onemsecond = cast('1 microsecond', Interval)
    oneday = cast('1 day', Interval)

    def period_stats(until, times, period):
        # For more info on how generate_series work to create the time intervals, please
        # refer to the documentation for logreports.build_query
        #
        # It's all PostgreSQL black magic and trickery ;-)

        oneinterval = cast('1 {}'.format(period), Interval)
        intervals = func.generate_series((until - (oneinterval * times)) + oneday, until, oneinterval).label('start')
        aliased_intervals = aliased(session.query(intervals).subquery(), 'timeperiod')

        start = aliased_intervals.c.start
        end   = (start + oneinterval) - onemsecond

        return (
            session.query(cast(start, Date).label("start"), cast(end, Date).label("end"),
                          func.sum(DiskFile.file_size).label("bytes"),
                          func.count(DiskFile.id).label("count"))
                    .select_from(aliased_intervals)
                    .outerjoin(Header, between(Header.ut_datetime, start, end))
                    .outerjoin(DiskFile)
                    .order_by(desc(start))
                    .group_by(start)
            )

    return dict(
        file_count    = session.query(File).count(),
        df_stats      = diskfile_stats,
        header_count  = session.query(Header).count(),
        iq_stats      = ingest_stats,
        daily_rates   = period_stats(until=comb, times=10, period='day'),
        weekly_rates  = period_stats(until=comb, times=6, period='week'),
        monthly_rates = period_stats(until=comb, times=6, period='month'),
        )

@templating.templated("statistics/content.html", with_generator=True)
def content():
    """
    Queries database for information concerning the total number and filesize of all stored files.
    Produces tables presenting the results, sorted by various properties such as instrument,
    observation class/type, and year of observation.
    """

    session = get_context().session

    # Presents total files and filesize
    filenum, filesize, datasize = (
        session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size))
                .filter(DiskFile.canonical == True)
                .one()
        )

    # This query takes canonical files grouped by telescope and instrument, and counts the
    # total files. This is the general query that we'll specialize to extract all the stats
    by_instrument_query = (
        session.query(Header.telescope, Header.instrument,
                      func.count().label("instnum"),                      # Total images taken by the instrument
                      func.sum(DiskFile.file_size).label("instbytes"),    # Total image sizes...
                      func.sum(DiskFile.data_size).label("instdata"),
                      func.sum(to_int(Header.engineering == True)).label("engnum"),
                      func.sum(to_int(Header.engineering == False)).label("scinum"),
                      func.sum(to_int(or_(Header.observation_class == 'science',
                                          Header.observation_class=='acq'))).label("sciacqnum"),
                      func.sum(to_int(or_(Header.observation_class=='progCal',
                                          Header.observation_class=='partnerCal',
                                          Header.observation_class=='acqCal',
                                          Header.observation_class=='dayCal'))).label("calacqnum"),
                      func.sum(to_int(Header.observation_type == 'OBJECT')).label("objnum")
                      )
                .select_from(join(DiskFile, Header))
                .filter(DiskFile.canonical == True)
                .filter(Header.telescope != None)
                .filter(Header.instrument != None)
                .group_by(Header.telescope, Header.instrument)
                .order_by(Header.telescope, Header.instrument)
        )

    # datetime variables and queries declared here
    # reject invalid 1969 type years by selecting post 1990
    firstyear = dt_date(1990, 0o1, 0o1)
    start = session.query(func.min(Header.ut_datetime)).filter(Header.ut_datetime > firstyear).one()[0]
    end = session.query(func.max(Header.ut_datetime)).select_from(join(Header, DiskFile)).filter(DiskFile.canonical == True).one()[0]

    # Select all combinations of (YEAR, TELESCOPE) to use in the following query
    tel_and_year = (
        session.query(distinct(Header.telescope).label("telescope"),
                      func.generate_series(start.year, end.year).label('year'))
                .filter(Header.telescope.isnot(None)).subquery()
        )

    # Build the query (file size and num grouped by telescope and year)
    extract_year = func.extract('YEAR', Header.ut_datetime)
    CANONICAL=to_int(DiskFile.canonical == True)
    by_year_query = (
        session.query(tel_and_year.c.telescope, tel_and_year.c.year,
                      func.sum(CANONICAL * DiskFile.file_size).label('yearbytes'),
                      func.sum(CANONICAL).label('yearnum'),
                      func.sum(CANONICAL * DiskFile.data_size).label('yeardata'))
                .outerjoin(Header, and_(tel_and_year.c.year == extract_year,
                                        tel_and_year.c.telescope == Header.telescope))
                .outerjoin(DiskFile)
                .group_by(tel_and_year.c.year, tel_and_year.c.telescope)
                .order_by(desc(tel_and_year.c.year), tel_and_year.c.telescope)
        )

    return dict(
        is_development = (fits_system_status == "development"),
        num_files      = filenum,
        size_files     = filesize,
        data_size      = datasize,
        by_instrument  = by_instrument_query,
        by_year        = by_year_query,
        )
