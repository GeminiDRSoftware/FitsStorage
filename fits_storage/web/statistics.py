import datetime

from sqlalchemy import desc, func, join, or_

from . import templating

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.queues.orm.ingestqueueentry import IngestQueueEntry
from fits_storage.queues.orm.exportqueueentry import ExportQueueEntry
from fits_storage.queues.orm.fileopsqueueentry import FileopsQueueEntry
from fits_storage.queues.orm.calcachequeueentry import CalCacheQueueEntry
from fits_storage.queues.orm.previewqueueentry import PreviewQueueEntry
from fits_storage.queues.orm.reducequeentry import ReduceQueueEntry

from fits_storage.db.query_utils import to_int
from fits_storage.server.wsgi.context import get_context

from fits_storage.web.user import needs_login

from fits_storage.config import get_config
fsc = get_config()


@needs_login(staff=True)
@templating.templated("statistics/stats.html")
def stats():
    """
    Provides live statistics on database: various file counts and total sizes,
    queue status, and datarate for various date ranges is queried.
    """

    session = get_context().session

    # DiskFile table statistics
    df_query = session.query(DiskFile)

    def number_of_entries_within(delta):
        return df_query.filter(DiskFile.entrytime >
                               (datetime.datetime.now() - delta)).count()

    diskfile_stats = {
        'total_rows': df_query.count(),
        'present_rows': df_query.filter(DiskFile.present == True).count(),
        'present_size': session.query(func.sum(DiskFile.file_size)).
        filter(DiskFile.present == True).one()[0],
        'latest': session.query(func.max(DiskFile.entrytime)).one()[0],
        'last_minute': number_of_entries_within(datetime.timedelta(minutes=1)),
        'last_hour': number_of_entries_within(datetime.timedelta(hours=1)),
        'last_day': number_of_entries_within(datetime.timedelta(days=1)),
        'last_entries': df_query.order_by(desc(DiskFile.entrytime)).limit(10)
    }

    # Queue stats. This is called directly when we build the return dict
    def queue_stats(orm):
        return {
            'count': session.query(orm).count(),
            'in_progress': session.query(orm).
            filter(orm.inprogress == True).count()
        }

    # Data rate statistics. This is called directly when we build the return

    def data_rate(utstart, utend):
        # Returns a dict(start=, count=, bytes=)
        count, totalsize = session.query(func.count(),
                                         func.sum(DiskFile.file_size)).\
            select_from(join(DiskFile, Header)). \
            filter(DiskFile.canonical == True). \
            filter(Header.ut_datetime.between(utstart, utend)).one()
        return {'start': utstart, 'count': count, 'bytes': totalsize}

    def do_six(start, interval):
        ret = []
        for i in range(6):
            end = start - interval
            ret.append(data_rate(start, end))
            start = end
        return ret

    today = datetime.datetime.utcnow().date()
    zerohour = datetime.time(0, 0, 0)
    todayzero = datetime.datetime.combine(today, zerohour)

    return {
        'file_count':    session.query(File).count(),
        'df_stats':      diskfile_stats,
        'header_count':  session.query(Header).count(),
        'iq_stats':      queue_stats(IngestQueueEntry),
        'eq_stats':      queue_stats(ExportQueueEntry),
        'pq_stats':      queue_stats(PreviewQueueEntry),
        'cq_stats':      queue_stats(CalCacheQueueEntry),
        'fq_stats':      queue_stats(FileopsQueueEntry),
        'rq_stats':      queue_stats(ReduceQueueEntry),
        'daily_rates':   do_six(todayzero, datetime.timedelta(days=1)),
        'weekly_rates':  do_six(todayzero, datetime.timedelta(days=7)),
        'monthly_rates': do_six(todayzero, datetime.timedelta(days=30)),
    }


@needs_login(staff=True)
@templating.templated("statistics/content.html", with_generator=True)
def content():
    """
    Queries database for information concerning the total number and filesize
    of all stored files, and generates tables presenting the results, sorted by
    various properties such as instrument, observation class/type, and year
    of observation.
    """

    session = get_context().session

    # Presents total files and filesize
    filenum, filesize, datasize = session.query(func.count(),
                                                func.sum(DiskFile.file_size),
                                                func.sum(DiskFile.data_size))\
        .filter(DiskFile.canonical == True).one()

    # This query takes canonical files grouped by telescope and instrument,
    # and counts the total files. This is the general query that we'll
    # specialize to extract all the stats
    by_instrument_query = (
        session.query(Header.telescope, Header.instrument,
                      func.count().label("instnum"),                      # Total images taken by the instrument
                      func.sum(DiskFile.file_size).label("instbytes"),    # Total image sizes...
                      func.sum(DiskFile.data_size).label("instdata"),
                      func.sum(to_int(Header.engineering == True)).label("engnum"),
                      func.sum(to_int(Header.engineering == False)).label("scinum"),
                      func.sum(to_int(or_(Header.observation_class == 'science',
                                          Header.observation_class == 'acq'))).label("sciacqnum"),
                      func.sum(to_int(or_(Header.observation_class == 'progCal',
                                          Header.observation_class == 'partnerCal',
                                          Header.observation_class == 'acqCal',
                                          Header.observation_class == 'dayCal'))).label("calacqnum"),
                      func.sum(to_int(Header.observation_type == 'OBJECT')).label("objnum")
                      )
                .select_from(join(DiskFile, Header))
                .filter(DiskFile.canonical == True)
                .filter(Header.telescope != None)
                .filter(Header.instrument != None)
                .group_by(Header.telescope, Header.instrument)
                .order_by(Header.telescope, Header.instrument)
        )

    telescopes = ['Gemini-North', 'Gemini-South']
    minyear = 2000
    maxyear = datetime.date.today().year

    # Build a list of dicts that we're going to populate
    lod = []
    for t in telescopes:
        for y in range(minyear, maxyear+1):
            lod.append({'telescope': t, 'year': y})

    for row in lod:
        startdate = datetime.datetime(row['year'], 1, 1)
        enddate = datetime.datetime(row['year']+1, 1, 1)
        query = session.query(func.count().label('num'),
                              func.sum(DiskFile.file_size).label('file_size'),
                              func.sum(DiskFile.data_size).label('data_size'))\
            .select_from(join(Header, DiskFile))\
            .filter(DiskFile.canonical == True)\
            .filter(Header.telescope == row['telescope'])\
            .filter(Header.ut_datetime.between(startdate, enddate))
        values = query.one()
        row['num'] = values['num']
        row['file_size'] = values['file_size']
        row['data_size'] = values['data_size']

    return {
        'is_development': (fsc.fits_system_status == "development"),
        'num_files':      filenum,
        'size_files':     filesize,
        'data_size':      datasize,
        'by_instrument':  by_instrument_query,
        'by_year':        lod
    }
