"""
This module handles the web 'logreports' functions - presenting data from the usage, query, upload and download logs
"""
import datetime
import dateutil.parser
from collections import namedtuple

from sqlalchemy import text
from sqlalchemy import and_, between, cast, desc, extract, func, join
from sqlalchemy import Date, Interval, String
from sqlalchemy.orm import aliased

from ..orm import pg_db
from ..orm import Base
from ..orm.usagelog import UsageLog
from ..orm.querylog import QueryLog
from ..orm.downloadlog import DownloadLog
from ..orm.filedownloadlog import FileDownloadLog
from ..orm.fileuploadlog import FileUploadLog
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.user import User

from ..utils.query_utils import to_int, null_to_zero
from ..utils.web import get_context, Return

from ..gemini_metadata_utils import ONEDAY_OFFSET

from .user import needs_login
from .selection import getselection, queryselection, sayselection
from . import templating

def logs(session, filter_func = None):
    aQueryLog = session.query(QueryLog, func.row_number().over(QueryLog.usagelog_id).label('row_number'))
    aDownloadLog = session.query(DownloadLog, func.row_number().over(DownloadLog.usagelog_id).label('row_number'))
    if filter_func:
        aQueryLog = filter_func(session, aQueryLog.join(UsageLog))
        aDownloadLog = filter_func(session, aDownloadLog.join(UsageLog))
    class AliasedQueryLog(Base):
        __table__ = aQueryLog.cte('query_log')
    class AliasedDownloadLog(Base):
        __table__ = aDownloadLog.cte('download_log')

    return AliasedQueryLog, AliasedDownloadLog

@needs_login(staffer=True)
@templating.templated("logreports/usagereport.html", with_generator=True)
def usagereport():
    """
    This is the usage report handler
    """

    # Process the form data if there is any
    # Default all the pre-fill strings
    # Default to last day
    today = datetime.datetime.utcnow().date()
    tomorrow = today + ONEDAY_OFFSET
    start = today.isoformat()
    end = tomorrow.isoformat()
    username = ''
    ipaddr = ''
    this = ''
    status = ''

    ctx = get_context()

    session = ctx.session

    formdata = ctx.get_form_data()
    for key, value in ((k, v.value) for (k, v) in list(formdata.items())):
        if key == 'start' and len(value):
            start = dateutil.parser.parse(value)
        elif key == 'end' and len(value):
            end = dateutil.parser.parse(value)
        elif key == 'username' and len(value):
            user = session.query(User).filter(User.username == value).first()
            if user:
                username = user.username
                user_id = user.id
        elif key == 'ipaddr' and len(value):
            ipaddr = str(value)
        elif key == 'this' and len(value):
            this = str(value)
        elif key == 'status' and len(value):
            try:
                status = int(value)
            except:
                pass

    template_args = dict(
        form=dict(start=start,
                  end=end,
                  user=username,
                  ip=ipaddr,
                  this=this,
                  status=status)
        )

    if formdata:
        def filter_usagelog(session, query):
            if start:
                query = query.filter(UsageLog.utdatetime >= start)
            if end:
                query = query.filter(UsageLog.utdatetime <= end)
            if username:
                user = session.query(User).filter(User.username == username).first()
                if user:
                    query = query.filter(UsageLog.user_id == user.id)
            if ipaddr:
                query = query.filter(UsageLog.ip_address == ipaddr)
            if this:
                query = query.filter(UsageLog.this == this)
            if status:
                try:
                    query = query.filter(UsageLog.status == int(status))
                except:
                    pass
            return query

        # Subquery to add a "row count" to the QueryLog and the DownloadLog. This is an easy way to pick just
        # the first relation when joining a one-to-many with potentially more than one result per match.
        # The underlying mechanism is the windowing capability of PostgreSQL (using the 'OVER ...' clause)
        aql, adl = logs(session, filter_usagelog)

        query = (
            session.query(UsageLog, aql, adl)
                   .outerjoin(aql, and_(aql.usagelog_id==UsageLog.id, aql.row_number == 1))
                   .outerjoin(adl, and_(adl.usagelog_id==UsageLog.id, adl.row_number == 1))
            )
        query = filter_usagelog(session, query)

        usagelogs = query.order_by(desc(UsageLog.utdatetime))

        if usagelogs.count() > 0:
            template_args['results'] = usagelogs

    return template_args

@needs_login(staffer=True)
@templating.templated("logreports/usagedetails.html")
def usagedetails(ulid):
    """
    This is the usage report detail handler, based on the numeric usagelog ID
    """

    session = get_context().session

    # Subquery to add a "row count" to the QueryLog and the DownloadLog. This is an easy way to pick just
    # the first relation when joining a one-to-many with potentially more than one result per match.
    # The underlying mechanism is the windowing capability of PostgreSQL (using the 'OVER ...' clause)
    def filter_usagelog(session, query):
        return query.filter(UsageLog.id == ulid)

    aql, adl = logs(session, filter_usagelog)
    usagelog, user, querylog, downloadlog = (
        session.query(UsageLog, User, aql, adl)
               .outerjoin(User, User.id == UsageLog.user_id)
               .outerjoin(aql, and_(aql.usagelog_id==UsageLog.id, aql.row_number == 1))
               .outerjoin(adl, and_(adl.usagelog_id==UsageLog.id, adl.row_number == 1))
               .filter(UsageLog.id == ulid).one()
        )

    filedownloadlogs = session.query(FileDownloadLog).filter(FileDownloadLog.usagelog_id==ulid)
    fileuploadlogs = session.query(FileUploadLog).filter(FileUploadLog.usagelog_id==ulid)

    return dict(
        ulog  = usagelog,
        user  = user,
        qlog  = querylog,
        dlog  = downloadlog,
        has_downloads = filedownloadlogs.count() > 0,
        fdlog = filedownloadlogs,
        has_uploads = fileuploadlogs.count() > 0,
        uplog = fileuploadlogs
        )

@needs_login(staffer=True)
@templating.templated("logreports/downloadlog.html", with_generator=True)
def downloadlog(patterns):
    """
    This accepts a list of filename patterns and returns a log showing all the downloads
    of the files that match the selection.

    "downloadlog" is a bit of a misnomer, maybe. The use case here is to let people
    know who, and when, has downloaded a certain file (or files).
    """

    def calc_permission(fdl):
        permission = ""
        if fdl.pi_access: permission += 'PI '
        if fdl.released: permission += 'Released '
        if fdl.staff_access: permission += 'Staff '
        if fdl.magic_access: permission += 'Magic '
        if fdl.eng_access: permission += 'Eng '
        if not fdl.canhaveit: permission += 'DENIED '

        return permission

    session = get_context().session

    hsq = session.query(Header, func.row_number().over(Header.diskfile_id).label('row_number')).subquery()
    aHeader = aliased(Header, hsq)
    dfsq = session.query(DiskFile, func.row_number().over(DiskFile.filename).label('row_number')).subquery()
    aDiskFile = aliased(DiskFile, dfsq)

    class Queries(object):
        def __init__(self, patterns):
            self.pt = patterns

        @property
        def empty(self):
            return len(self.pt) == 0

        @property
        def many(self):
            return len(self.pt) > 1

        def __iter__(self):
            for pattern in self.pt:
                yield (
                    pattern,
                    session.query(FileDownloadLog, User)#, aHeader.data_label)
                            .join(UsageLog)
                            .outerjoin(User)
                            .filter(FileDownloadLog.diskfile_filename.like(pattern + '%'))
                            .order_by(desc(FileDownloadLog.ut_datetime))
                    )

    return dict(
        permissions = calc_permission,
        queries = Queries(patterns)
        )

usagestats_header = """
<tr class='tr_head'>
<th></th>
<th colspan=2>Site Hits</th>
<th colspan=2>Searches</th>
<th colspan=2>PI Downloads</th>
<th colspan=2>Public Downloads</th>
<th colspan=2>Anonymous Downloads</th>
<th colspan=2>Staff Downloads</th>
<th colspan=2>Total Downloads</th>
<th>Failed Downloads</th>
<th colspan=2>Uploads</th>
</tr>
<tr class='tr_head'>
<th>Period</th>
<th>ok</th><th>fail</th>
<th>ok</th><th>fail</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>files</th><th>gb</th>
<th>number</th>
<th>files</th><th>gb</th>
</tr>
"""

@needs_login(staffer=True)
@templating.templated("logreports/usagestats.html", with_generator=True)
def usagestats():
    """
    Usage statistics:
    Site hits
    Searches
    Downloads:
      Proprietry data
      Public data logged in
      Public data not logged in
    Ingests

    Generate counts per year, per week and per day
    """

    session = get_context().session

    first, last = session.query(func.min(UsageLog.utdatetime),
                                func.max(UsageLog.utdatetime)).first()

    # General statistics breakdown
    groups = (('Per Year', build_query(session, 'year')),
              ('Per Week', build_query(session, 'week', first)),
              ('Per Day',  build_query(session, 'day', first)))

    end = datetime.datetime.utcnow()
    interval = datetime.timedelta(days=90)
    start = end - interval

    user_stats_sq = (
        session.query(UsageLog.user_id, func.count(1).label("downloads"))
                .filter(UsageLog.this=='searchform')
                .filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
                .group_by(UsageLog.user_id).order_by(desc(func.count(1)))
                .limit(10).subquery()
        )

    user_stats_1 = (
        session.query(user_stats_sq.c.downloads, User)
                .outerjoin(User, User.id == user_stats_sq.c.user_id)
        )

    user_stats_sq = (
        session.query(UsageLog.user_id, func.sum(UsageLog.bytes).label("bytes"))
            .filter(UsageLog.this=='download')
            .filter(UsageLog.utdatetime >= start).filter(UsageLog.utdatetime < end)
            .group_by(UsageLog.user_id).order_by(desc(func.sum(UsageLog.bytes)))
            .limit(10).subquery()
        )

    user_stats_2 = (
        session.query(user_stats_sq.c.bytes, User)
                .outerjoin(User, User.id == user_stats_sq.c.user_id)
        )

    return dict(
        groups      = groups,
        inquisitive = user_stats_1,
        hungry      = user_stats_2
        )

############################################################################################################

##     ##                            #                 #######
##     ##                           #                  ##     ##
##     ##  #####  # ###   #####     # ####   #####     ##     ## # ###   ####   ####   #####  # ###   #####
######### #     # ##   # #     #    ##    # #     #    ##     ## ##   #      # #    # #     # ##   # #
##     ## ######  #      ######     #     # ######     ##     ## #      #### # #    # #     # #    #  #####
##     ## #       #      #          #     # #          ##     ## #     #    ## #   ## #     # #    #       #
##     ##  #####  #       #####      #####   #####     #######   #      ######  ### #  #####  #    #  #####
                                                                                    #
                                                                                ####

############################################################################################################

# What follows is a function that builds a rather complicated SQL query, written to optimize the call to
# usagestats, and as a testbed for other optimizations. This optimization is not really that needed, because
# usagestats is not called often, but the original function triggered multiple database queries per rendered
# row, and I (Ricardo) wanted to remove that behaviour.
#
# Understanding it SEEMS not to easy, but that's just because of the size. We'll be providing plenty of
# documentation to let future maintainers know what to touch, and where.

UsageResult = namedtuple('UsageResult',
                (
                 'date',         # String representation of the summarized period
                 'hit_ok',       # Number of successful queries (HTTP Status 200)
                 'hit_fail',     # Number of non-successful queries
                 'search_ok',    # Number of successful queries involving /searchform
                 'search_fail',  # Number of non-successful queries involving /searchform
                 'total_down',   # Total downloaded files
                 'total_bytes',  # Total downloaded bytes
                 'up',           # Total uploaded files
                 'up_bytes',     # Total uploaded bytes
                 'pi_down',      # Total files downloaded by a PI user
                 'pi_bytes',     # Total bytes downloaded by a PI user
                 'staff_down',   # Total files downloaded by Gemini staff
                 'staff_bytes',  # Total bytes downloaded by Gemini staff
                 'public_down',  # Total released files downloaded by non-anonymous users
                 'public_bytes', # Total released-image bytes downloaded by non-anonymous users
                 'anon_down',    # Total files downloaded by an anonymous user
                 'anon_bytes',   # Total bytes downloaded by an anonymous user
                 'failed_down'   # Total failed downloads
                 ))


def build_query(session, period, since=None):
    """This generator creates a query to tally usage stats, grouped by `period` (which can be
       'year', 'week', or 'day'. The objective is to return a collection of UsageResult, one
       per period, sumarizing the corresponding statistics. The definition of the UsageResult
       namedtuple explains each field.

       Both 'week' and 'day' must speficy `since`, to limit the amount of returned data. 'year'
       will work over all the data set.

       This front facing method tries to use a helper call to use the materialized views to
       provide this data quickly.  If that fails, the call falls back to the older brute
       force query (which is slow).
       """
    try:
        partial = False
        for result in build_query_materialized_view(session, period, since):
            yield result
            partial = True
    except Exception as e:
        if not partial:
            # try the brute force approach
            build_query_brute_force(session, period, since)


def build_query_materialized_view(session, period, since=None):
    '''This generator creates a query to tally usage stats, grouped by `period` (which can be
       'year', 'week', or 'day'. The objective is to return a collection of UsageResult, one
       per period, sumarizing the corresponding statistics. The definition of the UsageResult
       namedtuple explains each field.

       Both 'week' and 'day' must speficy `since`, to limit the amount of returned data. 'year'
       will work over all the data set.'''
    conn = None
    try:
        conn = pg_db.connect()
        if period == "year":
            rs = conn.execute("select * from year_usage_stats order by yr desc")
        elif period == "week":
            stmt = text("select * from week_usage_stats where tme >= :query_date order by tme desc")
            stmt = stmt.bindparams(query_date=since)
            rs = conn.execute(stmt)
        elif period == "day":
            stmt = text("select * from day_usage_stats where tme >= :query_date order by tme desc")
            stmt = stmt.bindparams(query_date=since)
            rs = conn.execute(stmt)
        for result in rs:
            yield UsageResult(*result)
    finally:
        if conn:
            conn.close()


def build_query_brute_force(session, period, since=None):
    '''This generator creates a query to tally usage stats, grouped by `period` (which can be
       'year', 'week', or 'day'. The objective is to return a collection of UsageResult, one
       per period, sumarizing the corresponding statistics. The definition of the UsageResult
       namedtuple explains each field.

       Both 'week' and 'day' must speficy `since`, to limit the amount of returned data. 'year'
       will work over all the data set.'''

    # Note that we're using 'IS TRUE' here. This is a common theme across the whole query. The reason
    # for using 'IS' (identity) instead of '=' (equality) is that NULL values ARE NOT taken into account
    # for equality tests: we'd get a NULL out of it, and we want a boolean.
    #
    # Our database contains NULL in plenty of places where you'd expect to find False, so it makes sense
    # to use this test and be sure.
    RELEASED_FILE=to_int(FileDownloadLog.released.is_(True))

    # Subquery that summarizes downloads and bytes. This is needed because the relation between usagelog
    # and filedownloadlog is of one-to-many: filedownloadlog details the files (one per entry) for a
    # download query which shows only once in usagelog. If we wouldn't perform this subquery, when joining
    # usagelog on the left (we'll do it later), the final query would show more rows than expected,
    # resulting in bogus statistics.
    #
    # The subquery is rather simple, otherwise. The following code is roughly equivalent to:
    #
    #   (
    #    SELECT   ul.id AS ulid, pi_access, staff_access, COUNT(1) AS `count`, SUM(diskfile_file_size) AS bytes,
    #             SUM(released_file) AS released,
    #             SUM(released_file * diskfile_file_size) AS released_bytes
    #    FROM     filedownloadlog AS fdl JOIN usagelog AS ul ON fdl.usagelog_id = ul.id
    #    GROUP BY ul.id, fdl.pi_access, fdl.staff_access
    #   ) AS donwload_stats
    #
    # Note that 'released_file' is not a field in filedownloadlog. It's the operation represented by
    # RELEASED_FILE (see above), which is translated as:
    #
    #   CAST(filedownloadlog.released IS true AS integer)
    #
    # Which, as explained before, gets us a number, useful in sums and products. At the end of the day, this
    # query is giving us the following info:
    #
    #  - there was a petition for download                       (ul.id)
    #  - was it performed by a PI or Gemini staff?               (pi_access, staff_access - these are booleans)
    #  - how many files were downloaded in total?                (count)
    #  - how many bytes in total?                                (bytes)
    #  - how many of those files are out of proprietary period?  (released)
    #  - and how many bytes do those represent, you said?        (released_bytes)
    download_query = session.query(UsageLog.id.label('ulid'),
                                   FileDownloadLog.pi_access.label('pi_access'),
                                   FileDownloadLog.staff_access.label('staff_access'),
                                   func.count(FileDownloadLog.id).label('count'),
                                   func.sum(FileDownloadLog.diskfile_file_size).label('bytes'),
                                   func.sum(RELEASED_FILE).label('released'),
                                   func.sum(RELEASED_FILE * FileDownloadLog.diskfile_file_size).label('released_bytes'))\
                            .select_from(join(FileDownloadLog, UsageLog))\
                            .group_by(UsageLog.id, FileDownloadLog.pi_access,
                                                   FileDownloadLog.staff_access)\
                            .cte(name='download_stats')

    # Subquery that summarizes uploads and bytes. The rationale for this subquery would be the same as
    # for download_query, as the relationship between usagelog and fileuploadlog is technically a
    # one-to-many. In reality, though, the current implementation accepts only single files per upload,
    # so there's only one entry per upload. It doesn't hurt to generalize, though, and gives us an
    # appropriate target for the big fat JOIN that will be performed later. Plus, if we ever implement
    # uploading tarballs, we get that for free (aren't we smart?)
    #
    # This is basically equivalent to:
    #
    #   (
    #    SELECT   ul.id AS ulid, COUNT(1) as `count`, SUM(ful.bytes) AS bytes
    #    FROM     fileuploadlog AS ful JOIN usagelog AS ul ON ful.usagelog_id = ul.id
    #    GROUP BY ul.id
    #   ) AS upload_stats
    #
    # The name for the fields are equivalent to those of the download query; just substitute
    # 'downloaded' for 'uploaded'.
    upload_query = session.query(UsageLog.id.label('ulid'),
                                 func.count(FileUploadLog.id).label('count'),
                                 func.sum(FileUploadLog.size).label('bytes'))\
                          .select_from(join(FileUploadLog, UsageLog))\
                          .group_by(UsageLog.id)\
                          .cte(name='upload_stats')

    # Now, THIS unassuming query fragment is the core join that relates all the usage statistics.
    # It's equivalent to:
    #
    #    ...
    #    FROM (usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
    #                         LEFT JOIN upload_stats AS us ON ul.id = us.ulid
    #    ...
    #
    # Notice that we're doing Left Outer Joins here. This is VERY IMPORTANT. Doing a regular
    # Inner (natural) Join would return rows ONLY where a usagelog entry has a corresponding
    # download entry (or entries)... AND a corresponding upload entry.
    #
    # Which is impossible
    #
    # It would be bad enough even if we had only downloads, because we'de be limited to only
    # download queries, and we want all of them. In any case, what we get out of this join
    # operation is one row per usagelog entry, with (potentally) extra data if there was a
    # download or an upload. Otherwise, all those extra columns will be NULL, which is OK.
    the_join = join(join(UsageLog, download_query, UsageLog.id == download_query.c.ulid,
                         isouter=True),
                    upload_query, UsageLog.id == upload_query.c.ulid,
                    isouter=True)

    # Now comes the (potentially) most confusing part. We want to group the entries of the
    # join we just defined. And the grouping will be done according to one out of three
    # criteria:
    #
    #  - per year
    #  - per week (with the first day of the first week starting in the day passed in 'since',
    #              may not necessarily be Sunday - or Monday, for those of you in countries with
    #              a non-Sunday first day of the week)
    #  - per day
    #
    # To do this, in the following piece of code we create a master query that incorporates
    # the_join. Notice, though, that we're only querying for one column (the one that defines
    # the period for the row). That's OK. All the other columns are common to the different
    # queries, and we'll add them later using the `add_columns` method of the SQLAlchemy
    # query object.
    if period == 'year':
        # Simple enough. We extract the year component out of the usagelog.utdatetime, and
        # use that information to group the rows. Nothing complicated here.
        #
        # The equivalent query here would be then:
        #
        #    SELECT   EXTRACT(YEAR FROM utdatetime)
        #    FROM     (usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
        #                            LEFT JOIN upload_stats AS us ON ul.id = us.ulid)
        #    ORDER BY 1  -- Using the column position to avoid repeating the whole
        #    GROUP BY 1  -- EXTRACT(....)
        #
        ULYEAR = extract('YEAR', UsageLog.utdatetime)
        query = session.query(to_int(ULYEAR)).select_from(the_join).order_by(ULYEAR).group_by(ULYEAR)
    elif period in ('week', 'day'):
        # These two are a bit more complicated. It's easy to group by year, because it's
        # the slowest changing member of the date component... and never repeats (within
        # a specified calendar convention, that is)
        #
        # Week and day numbers, though, repeat rather often, meaning that we CANNOT use
        # them straight. Instead, we'll define an auxiliary 'period' table, with entries
        # defining the beginning and end of one. That will let us use the 'BETWEEN' operator
        # as grouping criterion.

        since = cast(since, Date) # Just to make sure that we have a date, not a timestamp

        # oneinterval is the only variable thing here. It depends on the input arguments
        # and can be one of:
        #
        #   - INTERVAL '1 week'
        #   - INTERVAL '1 day'
        oneinterval = cast('1 {0}'.format(period), Interval)
        onemsecond = cast('1 microsecond', Interval)

        # The following describes an aliased table (in SQLAlchemy terminology; for
        # PostgreSQL we would be talking about a CTE - Common Table Expression).
        # Also known as 'WITH query' Very useful to break down complicated queries
        # into simple ones.
        #
        # This one will prepare a temporary table of periods for us:
        #
        #    SELECT generate_series(first_date, last_date, INTERVAL '...') as start
        #
        # `start` is the name of the column in this subquery. `timeperiod` (the name
        # of the "aliased" table) will be used in the final query.
        intervals = func.generate_series(since, func.now(), oneinterval).label('start')
        aliased_intervals = aliased(session.query(intervals).subquery(), 'timeperiod')

        # Here we define the starting and ending points of a period. `start` comes
        # from the `start` column from timeperiod.
        # `end` is equivalent to (start + INTERVAL '...' - INTERVAL '1 microsecond').
        # We substract one microsecond because the operator 'BETWEEN' works on
        # closed ranges, meaning that it will include both ends. Substracting that
        # microsecond will get us a period like:
        #
        #  2015-02-14 00:00:00 - 2015-02-20 23:59:59.999999
        #
        # which should be more than enough precision for our needs. This won't work
        # if a query was placed during a leap second, but tough luck...
        start = aliased_intervals.c.start
        end = (start + oneinterval) - onemsecond

        # One more LEFT join! This one is to incorporate the whole list of periods
        # to the query. Again, we use a LEFT join to allow the retrieval of periods
        # with no activity whatsoever (an inner join would skip them). Equivalent to:
        #
        #   ... FROM timeperiod AS tp LEFT JOIN the_main_join AS tmj ON ul.utdatetime BETWEEN start AND end ...
        #
        # Here `ul` comes from the core JOIN that we defined befure, and `start` and `end`
        # are the expressions we just defined.
        more_join = join(aliased_intervals, the_join,
                         between(UsageLog.utdatetime, start, end),
                         isouter=True)

        # Finally, the query. This translates to:
        #
        #    WITH     (SELECT generate_series(first_date, last_date, INTERVAL '...') as start)
        #             AS timeperiod
        #    SELECT   (start || ' - ' || end)
        #    FROM     timeperiod AS tp LEFT JOIN
        #             ((usagelog AS ul LEFT JOIN download_stats AS ds ON ul.id = ds.ulid)
        #                              LEFT JOIN upload_stats AS us ON ul.id = us.ulid))
        #             ON ul.utdatetime BETWEEN start AND end
        #    ORDER BY start
        #    GROUP BY start
        #
        if period == 'week':
            period_element = cast(cast(start, Date), String) + ' - ' + cast(cast(end, Date), String)
        else:
            period_element = cast(start, Date)
        query = session.query(period_element)\
                       .select_from(more_join)\
                       .order_by(start)\
                       .group_by(start)
    else:
        raise RuntimeException('No valid period specified')

    # The rest of the the function defines some auxiliary terms that we'll use to build
    # the summarizing columns, which is what WE REALLY WANT to extract. They're not
    # complex and add nothing to the logic of the query. They're simply added to the
    # retrieved columns. All the information to figure out what info are we working with
    # has been described before.
    STATUS_200 = (UsageLog.status == 200)
    STATUS_FAIL = (UsageLog.status >= 400)
    THIS_SEARCH = (UsageLog.this == "searchform")
    HIT_OK = to_int(STATUS_200.is_(True))
    HIT_FAIL = to_int(STATUS_FAIL.is_(True))
    SEARCH_OK = to_int(and_(STATUS_200, THIS_SEARCH).is_(True))
    SEARCH_FAIL = to_int(and_(STATUS_FAIL.is_(True), THIS_SEARCH.is_(True)))

    DOWNLOAD_PERFORMED = and_(STATUS_200.is_(True), download_query.c.ulid.isnot(None)).is_(True)
    DOWNLOAD_FAILED    = and_(STATUS_FAIL.is_(True), download_query.c.ulid.isnot(None)).is_(True)
    UPLOAD_PERFORMED   = and_(STATUS_200.is_(True), upload_query.c.ulid.isnot(None)).is_(True)

    FILE_COUNT = to_int(null_to_zero(download_query.c.count))
    PUBFILE_COUNT = to_int(null_to_zero(download_query.c.released))
    DOWNBYTE_COUNT = to_int(null_to_zero(download_query.c.bytes), big=True)
    UPBYTE_COUNT = to_int(null_to_zero(upload_query.c.bytes), big=True)

    COUNT_DOWNLOAD = to_int(DOWNLOAD_PERFORMED) * FILE_COUNT
    COUNT_FAILED = to_int(DOWNLOAD_FAILED)
    COUNT_UPLOAD = to_int(UPLOAD_PERFORMED)
    PI_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, download_query.c.pi_access.is_(True)))
    STAFF_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, download_query.c.staff_access.is_(True)))
    ANON_DOWNLOAD = to_int(and_(DOWNLOAD_PERFORMED, UsageLog.user_id.is_(None)))

    TOTAL_BYTES = to_int(null_to_zero(UsageLog.bytes), big=True)
    PUBFILE_BYTES = to_int(null_to_zero(download_query.c.released_bytes), big=True)

    q = query.add_columns(func.sum(HIT_OK).label('hits_ok'), func.sum(HIT_FAIL).label('hits_fail'),
                          func.sum(SEARCH_OK).label('search_ok'), func.sum(SEARCH_FAIL).label('search_fail'),
                          func.sum(COUNT_DOWNLOAD).label('downloads_total'), func.sum(DOWNBYTE_COUNT).label('bytes_total'),
                          func.sum(COUNT_UPLOAD).label('uploads_total'), func.sum(UPBYTE_COUNT).label('ul_bytes_total'),
                          func.sum(PI_DOWNLOAD * FILE_COUNT).label('pi_downloads'), func.sum(PI_DOWNLOAD * DOWNBYTE_COUNT).label('pi_dl_bytes'),
                          func.sum(STAFF_DOWNLOAD * FILE_COUNT).label('staff_downloads'), func.sum(STAFF_DOWNLOAD * DOWNBYTE_COUNT).label('staff_dl_bytes'),
                          func.sum(PUBFILE_COUNT).label('public_downloads'), func.sum(PUBFILE_BYTES).label('public_dl_bytes'),
                          func.sum(ANON_DOWNLOAD * FILE_COUNT).label('anon_downloads'), func.sum(ANON_DOWNLOAD * DOWNBYTE_COUNT).label('anon_dl_bytes'),
                          func.sum(COUNT_FAILED).label('failed'))

    # Yield the results. Yay! This function is a generator ;-)
    for result in q:
        yield UsageResult(*result)
