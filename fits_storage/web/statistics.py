"""
This is a script which defines functions for generating content, general, and possibly usage statistics on the database. It queries the database for stats and outputs them as HTML via the apachehandler.
"""

from sqlalchemy import desc, func, join, and_, or_, cast, between
from sqlalchemy import Interval, Date
from sqlalchemy.orm import aliased

from ..fits_storage_config import fits_system_status
from ..orm import sessionfactory
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.ingestqueue import IngestQueue

from . import templating

from ..apache_return_codes import HTTP_OK

from datetime import datetime, timedelta, time as dt_time, date as dt_date
from collections import defaultdict, namedtuple

@templating.templated("statistics/stats.html", with_session=True)
def stats(session, req):
    """
    Provides live statistics on fits database: total filesize, ingest queue status, and datarate for various date ranges is queried. Information is
    presented in html in the browser in a list format.
    """

    try:
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
    except IOError:
        pass

@templating.templated("statistics/content.html", with_session=True, with_generator=True)
def content(session, req):
    """
    Queries database for information concerning the total number and filesize of all stored files.
    Produces tables presenting the results, sorted by various properties such as instrument,
    observation class/type, and year of observation.
    """

    # Presents total files and filesize
    filenum, filesize, datasize = (
        session.query(func.count(), func.sum(DiskFile.file_size), func.sum(DiskFile.data_size))
                .filter(DiskFile.canonical == True)
                .one()
        )

    # build the telescope list
    query = session.query(Header.telescope).group_by(Header.telescope).order_by(Header.telescope)
    # results comes back as a list of one element tuples - clean up to a simple list
    tels = [tel for (tel,) in query if tel is not None]

    # This query takes canonical files grouped by telescope and instrument, and counts the
    # total files. This is the general query that we'll specialize to extract all the stats
    general_query = (
        session.query(Header.telescope, Header.instrument, func.count())
                .select_from(join(DiskFile, Header))
                .filter(DiskFile.canonical == True)
                .filter(Header.telescope != None)
                .filter(Header.instrument != None)
                .group_by(Header.telescope, Header.instrument)
        )

    # Specialized queries. Query objects are 'lazy', meaning that they will only be performed when we try to
    # extract rows from them
    filesizesq = general_query.add_columns(func.sum(DiskFile.file_size), func.sum(DiskFile.data_size))
    engq = general_query.filter(Header.engineering == True)
    sciq = general_query.filter(Header.engineering == False)
    classq = general_query.filter(or_(Header.observation_class == 'science',
                                          Header.observation_class=='acq'))
    calq = general_query.filter(or_(Header.observation_class=='progCal',
                                        Header.observation_class=='partnerCal',
                                        Header.observation_class=='acqCal',
                                        Header.observation_class=='dayCal'))
    typeq = general_query.filter(Header.observation_type == 'OBJECT')

    class Result(object):
        def __init__(self):
            self.instnum = 0
            self.instbytes = 0
            self.instdata = 0
            self.engnum = 0
            self.scinum = 0
            self.sciacqnum = 0
            self.calacqnum = 0
            self.objnum = 0

    # Build up the results
    results_inst = defaultdict(Result)

    for tel, instr, cnt, fs, ds in filesizesq:
        obj = results_inst[(tel, instr)]
        obj.instnum = cnt
        obj.instbytes = fs
        obj.instdata = ds

    pairs = ((engq, 'engnum'),
             (sciq, 'scinum'),
             (classq, 'sciacqnum'),
             (calq, 'calacqnum'),
             (typeq, 'objnum'))
    for qry, field in pairs:
        for tel, instr, cnt in qry:
            obj = results_inst[(tel, instr)]
            setattr(obj, field, cnt)

    # datetime variables and queries declared here
    # reject invalid 1969 type years by selecting post 1990
    firstyear = dt_date(1990, 01, 01)
    start = session.query(func.min(Header.ut_datetime)).filter(Header.ut_datetime > firstyear).first()[0]
    end = session.query(func.max(Header.ut_datetime)).select_from(join(Header, DiskFile)).filter(DiskFile.canonical == True).first()[0]

    # Build the query (file size and num grouped by telescope and year)
    extract_year = func.extract('YEAR', Header.ut_datetime)
    yearquery = session.query(Header.telescope, extract_year, func.sum(DiskFile.file_size), func.count(), func.sum(DiskFile.data_size))\
                            .select_from(join(Header, DiskFile))\
                            .filter(DiskFile.canonical == True)\
                            .filter(Header.ut_datetime >= firstyear)\
                            .group_by(Header.telescope, extract_year)\
                            .order_by(desc(extract_year), Header.telescope)

    # Extract data
    results_year = defaultdict(dict)

    for tel, year, yearbytes, yearnum, yeardata in yearquery:
        if tel is None:
            continue
        results_year[int(year)][tel] = (yearbytes, yearnum, yeardata)

    return dict(
        is_development = (fits_system_status == "development"),
        num_files      = filenum,
        size_files     = filesize,
        data_size      = datasize,
        telescopes     = tels,
        by_instrument  = sorted(results_inst.items()),
        by_year        = sorted(results_year.items(), reverse=True),
        )
