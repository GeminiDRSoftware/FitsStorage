import re
import time
import datetime
from datetime import date, timedelta
import dateutil.parser

DATE_LIMIT_LOW = dateutil.parser.parse('19900101')
DATE_LIMIT_HIGH = dateutil.parser.parse('20500101')
ZERO_OFFSET = datetime.timedelta()
ONEDAY_OFFSET = datetime.timedelta(days=1)
UT_DATETIME_SECS_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)


def get_fake_ut(transit="14:00:00"):
    """
    Generate the fake UT date used to name Gemini data.

    At Gemini the transit time is set to 14:00:00 local time.  For GN, that
    corresponds to midnight UT so the dataset name is not faked, but for
    GS, a transit of 14hr is totally artificial.

    Before transit, UT of last night
    After transit, UT of coming night

    Note that the transit time is not hardcoded and the code should continue
    to work if the Gemini's policy regarding the transit time were to change.

    Parameters
    ----------
    transit : <str>
        UT transit time to use.  Format: "hh:mm:ss".  Default: "14:00:00"

    Returns
    -------
    fake_ut: <str>
        Formatted date string: 'yyyymmdd'

    --------
    Original author:  Kathleen Labrie  31.10.2008  Based on CL script.
    Original   code:  gempylocal.ops_suppor.ops_utils.get_fake_ut().

    """
    # Convert the transit time string into a datetime.time object
    transittime = datetime.datetime.strptime(transit, "%H:%M:%S").time()

    # Get the local and UTC date and time
    dtlocal = datetime.datetime.now()
    dtutc = datetime.datetime.utcnow()

    # Generate the fake UT date
    if dtlocal.time() < transittime:
        # Before transit
        if dtutc.date() == dtlocal.date():
            fake_ut = ''.join(str(dtutc.date()).split('-'))
        else:
            # UT has changed before transit => fake the UT
            oneday = datetime.timedelta(days=1)
            fake_ut = ''.join(str(dtutc.date() - oneday).split('-'))
    else:
        # After or at transit
        if dtutc.date() == dtlocal.date():
            # UT has not changed yet; transit reached => fake the UT
            oneday = datetime.timedelta(days=1)
            fake_ut = ''.join(str(dtutc.date() + oneday).split('-'))
        else:
            fake_ut = ''.join(str(dtutc.date()).split('-'))

    return fake_ut


def gemini_date(string, as_datetime=False, offset=ZERO_OFFSET):
    """
    A utility function for matching dates of the form YYYYMMDD
    also supports today/tonight, yesterday/lastnight
    returns the YYYYMMDD string, or '' if not a date.

    Parameters
    ----------
    string: <str>
        A string moniker indicating a day to convert to a gemini_date.
        One of 'today', tomorrow', 'yesterday', 'lastnight' OR an actual
        'yyyymmdd' string.

    as_datetime: <bool>
        return is a datetime object.
        Default is False

    offset: <datetime>
        timezone offset from UT.
        default is ZERO_OFFSET

    Returns
    -------
    <datetime>, <str>, <NoneType>
        One of a datetime object; a Gemini date of the form 'YYYYMMDD';
        None.

    """
    suffix = ''
    if string.endswith('Z'):
        # explicit request for UTC, set offset to zero
        string = string[:-1]
        offset = ZERO_OFFSET
        suffix = 'Z'

    dt_to_text = lambda x: x.date().strftime('%Y%m%d') + suffix
    dt_to_text_full = lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + suffix

    if string in {'today', 'tonight'}:
        string = get_fake_ut()
        # string = dt_to_text(datetime.datetime.utcnow())
    elif string in {'yesterday', 'lastnight'}:
        past = dateutil.parser.parse(get_fake_ut()) - ONEDAY_OFFSET
        string = dt_to_text(past)
        # string = dt_to_text(datetime.datetime.utcnow() - ONEDAY_OFFSET)

    if len(string) == 8 and string.isdigit():
        # What we want here is to bracket from 2pm yesterday through 2pm today.
        # That is, 20200415 should convert to 2020-04-14 14:00 local time, but
        # in UTC.  The offset we are passed is what we need to add, including
        # the 2pm offset as well as the timezone adjustment to convert back to
        # UTC.
        # Example (HST): 2020-04-15 0:00 -10 hrs =
        #                    2020-04-14 2pm + 10 hrs = 2020-04-15 0:00
        # Example (CL): 2020-04-15 0:00 -10 hrs =
        #                    2020-04-14 2pm + 4 hrs = 2020-04-14 18:00
        # offset (HST) = -10 + 10 = 0
        # offset (CL) = -10 + 4 = -6
        try:
            dt = dateutil.parser.parse(string) + offset
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text(dt) if not as_datetime else dt
        except ValueError:
            pass

    if len(string) >= 14 and 'T' in string and ':' in string and \
            '=' not in string:
        # Parse an ISO style datestring, so 2019-12-10T11:22:33.444444
        try:
            # TODO this is dateutil bug #786, so for now we truncate to 6 digits
            if '.' in string:
                lastdot = string.rindex('.')
                if len(string) - lastdot > 6:
                    string = string[:lastdot - len(string)]
            # TODO end of workaround
            dt = dateutil.parser.isoparse("%sZ" % string) + offset
            # strip out time zone as the rest of the code does not support it
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text_full(dt) if not as_datetime else dt
        except ValueError:
            pass

    if len(string) >= 14 and 'T' in string and ':' not in string and \
            '=' not in string and '-' not in string:
        # Parse a compressed style datestring, so 20191210T112233
        try:
            dt = dateutil.parser.isoparse("%s-%s-%sT%s:%s:%sZ" %
                                          (string[0:4], string[4:6],
                                           string[6:8], string[9:11],
                                           string[11:13], string[13:15]))
            # strip  out time zone as the rest of the code does not support it
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text_full(dt) if not as_datetime else dt
        except ValueError:
            pass

    return '' if not as_datetime else None


def gemini_daterange(string, as_datetime=False, offset=ZERO_OFFSET):
    """
    A utility function for matching date ranges of the form YYYYMMDD-YYYYMMDD
    Does not support 'today', yesterday', ...

    Also this does not yet check for sensible date ordering returns the
    YYYYMMDD-YYYYMMDD string, or '' if not a daterange.

    Parameters
    ----------
    string: <str>
        date range of the form YYYYMMDD-YYYYMMDD.

    as_datetime: <bool>
        If True, return a recognized daterange as a pair of datetime objects,
        None if it's not a daterange.
        Default is False.

    offset: <datetime>
        timezone offset from UT.
        default is ZERO_OFFSET

    Returns
    -------
    <datetime>, <str>, <NoneType>
        One of a <datetime> object; a Gemini date of the form 'YYYYMMDD';
        None.

    """
    datea, sep, dateb = string.partition('-')
    da = gemini_date(datea, as_datetime=True, offset=offset)
    db = gemini_date(dateb, as_datetime=True, offset=offset)
    if da and db:
        if as_datetime:
            return da, db

        return string

    return '' if not as_datetime else None


def get_date_offset() -> timedelta:
    """
    This function is used to add set offsets to the dates. The aim is to get
    the "current date" adjusting for the local time, taking into account the
    different sites where Gemini is based.

    Returns
    -------
    timedelta
        The `timedelta` to use for this application/server.
    """

    # if db_config.use_utc:
    #    return ZERO_OFFSET

    # Calculate the proper offset to add to the date
    # We consider the night boundary to be 14:00 local time
    # This is midnight UTC in Hawaii, completely arbitrary in Chile
    zone = time.altzone if time.daylight else time.timezone
    # print datetime.timedelta(hours=16)
    # print datetime.timedelta(seconds=zone)
    # print ONEDAY_OFFSET

    # return datetime.timedelta(hours=16) + datetime.timedelta(seconds=zone)
    # - ONEDAY_OFFSET
    # I think this interacted with the Chile today changes to cause the
    # missing starts in Hawaii for summary
    return datetime.timedelta(hours=14) + datetime.timedelta(seconds=zone)\
        - ONEDAY_OFFSET


def get_time_period(start: str, end: str = None, as_date: bool = False):
    """
    Get a time period from a given start and end date string.  The string
    format for the inputs is YYYYMMDD or YYYY-MM-DDThh:mm:ss.

    Parameters
    ----------
    start : str
        Start of the time period
    end : str
        End of the time period
    as_date: bool
        If True, make return type `date` only, else return full `datetime`
        objects, defaults to False

    Returns
    -------
    tuple
        A tuple of `date` or `datetime` with the resulting parsed values,
        defaults to False
    """
    startdt = gemini_date(start, offset=get_date_offset(), as_datetime=True)
    if end is None:
        enddt = startdt
    else:
        enddt = gemini_date(end, offset=get_date_offset(), as_datetime=True)
        # Flip them round if reversed
        if startdt > enddt:
            startdt, enddt = enddt, startdt
    if end is None or 'T' not in end:
        # day value, need to +1
        enddt += ONEDAY_OFFSET

    if as_date:
        return startdt.date(), enddt.date()

    return startdt, enddt


def gemini_time_period_from_range(rng: str, as_date: bool = False):
    """
    Get a time period from a passed in string representation

    Parameters
    ----------
    rng : str
        YYYYMMDD-YYYYMMDD style range
    as_date : bool
        If True, return tuple of `date`, else tuple of `datetime`, defaults to
        False

    Returns
    -------
    `tuple` of `datetime` or `tuple` of `date`
        Start and stop time of the period as `date` or `datetime` per `as_date`
    """
    a, _, b = gemini_daterange(rng).partition('-')
    return get_time_period(a, b, as_date)


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
