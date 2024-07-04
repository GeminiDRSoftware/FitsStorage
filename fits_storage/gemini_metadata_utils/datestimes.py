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
    A utility function for matching strings specifying dates of the form
    YYYYMMDD. Also supports special values today, yesterday, tomorrow
    returns None if it can't parse the string, the YYYYMMDD string, or a
    datetime.datetime instance is as_date is True.

    Parameters
    ----------
    string: <str>
        A string giving a date to parse.
        One of 'today', tomorrow', 'yesterday', or an actual 'YYYYMMDD' string.

    as_date: <bool>
        return a datetime.date object.
        Default is False, returns a YYYYMMDD string

    Returns
    -------
    <date>, <str>, <NoneType>
        One of a 'datetime.date' object, a string of the form 'YYYYMMDD',
        or None.

    """

    # This first if-elif... block ends with dt set to a datetime.date.

    # Handle the 'special values' first
    if string in ('yesterday', 'today', 'tomorrow'):
        now = datetime.datetime.utcnow()
        if string == 'today':
            dt = now
        elif string == 'yesterday':
            dt = now - ONEDAY_OFFSET
        elif string == 'tomorrow':
            dt = now + ONEDAY_OFFSET
        else:
            return None  # To prevent warnings about dt being unset
        dt = dt.replace(tzinfo=None).date()

    # Now handle YYYYMMDD
    elif len(string) == 8:
        try:
            dt = dateutil.parser.parse(string)
        except ValueError:
            return None

        dt = dt.replace(tzinfo=None).date()
        if dt < DATE_LIMIT_LOW.date() or dt > DATE_LIMIT_HIGH.date():
            return None

    else:
        return None

    if as_date:
        return dt
    else:
        return dt.strftime('%Y%m%d')


def gemini_daterange(string, as_dates=False):
    """
    A utility function for matching and parsing date ranges. These
    are of the form YYYYMMDD-YYYYMMDD

    Parameters
    ----------
    string: <str>
        date range of the form YYYYMMDD-YYYYMMDD.

    as_dates: <bool>
        Default is False. If True, return a pair of datetime.date objects,
        otherwise returns a pair of string of the form 'YYYYMMDD', 'YYYYMMDD'

    Returns
    -------
        One of:
        None, if the string cannot be parsed,
        a (<date>, <date>) pair, if as_dates is true,
        otherwise, a pair of strings of the form 'yyyymmdd', 'YYYYMMDD'
    """

    datea, sep, dateb = string.partition('-')
    if sep != '-' or datea is None or dateb is None:
        return None

    da = gemini_date(datea, as_date=True)
    db = gemini_date(dateb, as_date=True)
    if da is None or db is None:
        return None

    if da > db:
        # They're reversed, flip them round
        da, db = db, da

    if as_dates:
        return da, db

    return da.strftime('%Y%m%d'), db.strftime('%Y%m%d')


def get_time_period(start, end=None):
    """
    Get a start and end datetimes for a time period described by start and end.
    start and end can be strings of the form YYYYMMDD or datetime.dates.
    If end is not given, it is assumed to be the same as start.
    The returned values will be datetimes representing 00:00:00 on the start
    date and 00:00:00 on the day after the end date.

    Parameters
    ----------
    start : str or datetime.date
        Start day of the time period
    end : str or datetime.date
        End day of the time period

    Returns
    -------
    A tuple of `datetime` with the resulting parsed values, or None if we
    cannot parse the values.
    """

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
