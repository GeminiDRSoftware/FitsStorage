import re
import datetime
import dateutil.parser

DATE_LIMIT_LOW = datetime.datetime(1999, 1, 1, 0, 0, 0)
DATE_LIMIT_HIGH = datetime.datetime(2100, 1, 1, 0, 0, 0)
UT_DATETIME_SECS_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)
ONEDAY_OFFSET = datetime.timedelta(days=1)

# In Hawaii, where local time is HST == UTC-10, the UTC date rolls over at
# 14:00 local time, and thus the UTC date makes a very convenient "Night label"
# as the UTC date does not change during the observing night hours. In Chile,
# where local time is CLT == UTC-4 (standard) or CLST == UTC-3 (summer), this is
# not the case. In order to create a "Night Label" for Gemini South data, we
# apply a timedelta of - 6 Hours to UTC, which gives us a "timezone" in which
# the date doesn't change during a Gemini South observing night. This offset
# value is used in selection.py when querying by night.
CHILE_OFFSET = datetime.timedelta(hours=-6)


def gemini_date(string, as_date=False):
    """
    A utility function for matching strings specifying dates. Supports special
    values 'today', 'yesterday', 'tomorrow'. Initially intended to support
    values of the form YYYYMMDD, but now supports ISO8601 formats.

    Returns None if it can't parse the string.
    Returns an ISO compliant YYYYMMDD string, or a datetime.date instance
    if as_date is True.

    Parameters
    ----------
    string: <str>
        A string giving a date to parse.
        One of 'now', 'today', tomorrow', 'yesterday', or an ISO8601 compliant
        string.

    as_date: <bool>
        return a datetime.date or datetime.datetime object, depending on if the
        string passed was a date or datetime.
        Default is False, returns a YYYYMMDD string

    Returns
    -------
    <date>, <str>, <NoneType>
        One of a 'datetime.date' object, a string of the form 'YYYYMMDD',
        or None.

    """

    # This first if-elif... block ends with dt set to a datetime.date.

    # Handle the 'special values' first
    if string in ('now', 'yesterday', 'today', 'tomorrow'):
        now = datetime.datetime.utcnow()
        if string in ('now', 'today'):
            dt = now
        elif string == 'yesterday':
            dt = now - ONEDAY_OFFSET
        elif string == 'tomorrow':
            dt = now + ONEDAY_OFFSET
        else:
            return None  # To prevent warnings about dt being unset
        if string == 'now':
            dt = dt.replace(tzinfo=None)
        else:
            dt = dt.replace(tzinfo=None).date()

    # Now handle isoformat dates or datetimes
    else:
        try:
            dt = dateutil.parser.isoparse(string)
        except ValueError:
            return None

        if dt < DATE_LIMIT_LOW or dt > DATE_LIMIT_HIGH:
            return None

        # Figure out if we got a date or datetime
        dd = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if dt == dd:
            dt = dt.date()

    if as_date:
        return dt
    else:
        if isinstance(dt, datetime.date):
            return dt.strftime('%Y%m%d')
        else:
            return dt.strftime('%Y%m%dT%H%M%S')


def gemini_daterange(string, as_dates=False):
    """
    A utility function for matching and parsing date ranges. To maintain
    compatibility with previous formats, this is a little subtle.

    The new recommended format is: <iso1>--<iso2> where <iso1> and <iso2> are
    ISO format dates or datetimes, and the separator is a double hyphen,
    following ISO8601.

    If no '--' is found in the string, and only one '-' is found, then we
    interpret it as <iso1>-<iso2>. This precludes use of iso format date[time]s
    that include '-'s in their format. This includes the previous format.

    Note that YYYYMMDD is a valid ISO format date, and YYYYMMDDThhmmss[.s...]
    is a valid ISO format datetime.

    Parameters
    ----------
    string: <str>
        date range as specified above

    as_dates: <bool>
        Default is False, in which case return a string of the form
        'YYYYMMDD-YYYYMMDD'.
        If True, return a pair of datetime.date objects if the input is
        of the form 'YYYYMMDD-YYYYMMDD' or a pair of datetime.datetime objects
        if the input is the date/time ISO format.

    Returns
    -------
        One of:
        None, if the string cannot be parsed,
        a string of the form 'YYYYMMDD-YYYYMMDD' if as_dates is False
        a (<date>, <date>) pair, if as_dates is true and YYYYMMDD format input,
        a (<datetime>, <datetime> pair, if as_dates is true and ISO format input
    """

    parts = string.split('--')
    if len(parts) != 2:
        parts = string.split('-')
    if len(parts) != 2:
        return None

    da = gemini_date(parts[0], as_date=True)
    db = gemini_date(parts[1], as_date=True)

    if da is None or db is None:
        return None

    if da > db:
        # They're reversed, flip them round
        da, db = db, da

    if as_dates:
        return da, db

    return string


def get_time_period(start, end=None):
    """
    Get a start and end datetimes for a time period described by start and end.
    start and end can be strings of the form YYYYMMDD or datetime.date
    instances or datetime.datetime instances.
    If end is not given, it is assumed to be the same as start.
    If the inputs are datetime.datetime instances, they are returned unmodified.
    If the inputs are datetime.date instances or strings representing dates,
    the returned values will be datetimes representing 00:00:00 on the start
    date and 00:00:00 on the day after the end date.

    Parameters
    ----------
    start : str or datetime.date or datetime.datetime
        Start day of the time period
    end : str or datetime.date or datetime.datetime
        End day of the time period

    Returns
    -------
    A tuple of `datetime` with the resulting parsed values, or None if we
    cannot parse the values.
    """

    if isinstance(start, datetime.datetime) and \
            isinstance(end, datetime.datetime):
        return start, end

    if isinstance(start, datetime.date):
        startd = start
    else:
        startd = gemini_date(start, as_date=True)
        if startd is None:
            return None

    if end is None:
        endd = startd
    elif isinstance(end, datetime.date):
        endd = end
    else:
        endd = gemini_date(end, as_date=True)
        if endd is None:
            return None

        # Flip them round if reversed
        if startd > endd:
            startd, endd = endd, startd

    # Make them into datetimes at 00:00:00 on that day
    t = datetime.time(0, 0)
    startdt = datetime.datetime.combine(startd, t)
    enddt = datetime.datetime.combine(endd, t)

    # Advance enddt to exactly one day later
    enddt = enddt + datetime.timedelta(days=1)

    return startdt, enddt


def gemini_semester(dt):
    """
    Return the semester name that contains date

    Parameters
    ----------
    dt : date
        The date to check for the owning semester.

    Returns
    -------
    str
        Semester code containing the provided date.
    """
    if 2 <= dt.month <= 7:
        letter = 'A'
        year = dt.year
    else:
        letter = 'B'
        if dt.month == 1:
            year = dt.year - 1
        else:
            year = dt.year

    return str(year) + letter


_semester_re = r'(20\d\d)([AB])'


def previous_semester(semester: str):
    """
    Given a semester string, e.g., 2016A, return the previous semester.
    E.g., 2015B

    Parameters
    ----------
    semester: <str> Semester name

    Returns
    -------
    semester - 1, <str>,
        the semester name prior to the passed semester.

    """
    m = re.match(_semester_re, semester)
    if m is None:
        return None
    year = m.group(1)
    bsem = m.group(2) == 'B'

    if bsem:
        return year + 'A'
    else:
        year = int(year) - 1
        return str(year) + 'B'


def site_monitor(string: str) -> bool:
    """
    Parameters
    ----------
    string: <str>
        The name of the instrument that is a sky monitor. Currently, this
        supports only GS_ALLSKYCAMERA. The string will generally be that
        returned by the astrodata descriptor, ad.instrument().

    Returns
    -------
    bool
        Returns True when GS_ALLSKYCAMERA is passed.
    """
    if string == 'GS_ALLSKYCAMERA':
        return True
    else:
        return False
