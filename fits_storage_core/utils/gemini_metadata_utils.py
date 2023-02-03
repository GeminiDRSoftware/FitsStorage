"""
This is the gemini_metadata_utils module. It provides a number of utility
classes and functions for parsing the metadata in Gemini FITS files.

"""
from astropy.coordinates import Angle
from typing import Union, Tuple

import re
import time
import datetime
from datetime import date, timedelta
import dateutil.parser

# from . import fits_storage_config
from gemini_obs_db import db_config


__all__ = [
    "gemini_gain_settings",
    "gemini_readspeed_settings",
    "gemini_welldepth_settings",
    "gemini_readmode_settings",
    "gemini_telescopes",
    "gemini_telescope",
    "gemini_instrument",
    "get_fake_ut",
    "gemini_date",
    "ratodeg",
    "dectodeg",
    "degtora",
    "degtodec",
    "dmstodeg",
    "srtodeg",
    "gemini_daterange",
    "gemini_procmode",
    "obs_types",
    "gemini_observation_type",
    "obs_classes",
    "gemini_observation_class",
    "reduction_states",
    "gemini_reduction_state",
    "cal_types",
    "gemini_caltype",
    "gmos_gratings",
    "gmos_gratingname",
    "gmos_facility_plane_masks",
    "gmos_focal_plane_mask",
    "gemini_fitsfilename",
    "gemini_binning",
    "GeminiDataLabel",
    "GeminiObservation",
    "GeminiProgram",
    "get_date_offset",
    "get_time_period",
    "gemini_time_period_from_range",
    "gemini_semester",
    "previous_semester",
    "site_monitor",
    "UT_DATETIME_SECS_EPOCH",
]

# ------------------------------------------------------------------------------
DATE_LIMIT_LOW = dateutil.parser.parse('19900101')
DATE_LIMIT_HIGH = dateutil.parser.parse('20500101')
ZERO_OFFSET = datetime.timedelta()
ONEDAY_OFFSET = datetime.timedelta(days=1)
UT_DATETIME_SECS_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)
# ------------------------------------------------------------------------------
# Compile some regular expressions here. This is fairly complex, so I've
# split it up in substrings to make it easier to follow.
# Also these substrings are used directly by the classes

# This re matches a program id like GN-CAL20091020 with no groups
calengre_old = r'G[NS]-((?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'
calengre = r'^G[NS]?-20\d\d[ABFDLWVSX]-((?:CAL)|(?:ENG))-([A-Za-z0-9_]*[A-Za-z_]+[A-Za-z0-9_]*-)?\d+'

# G-YYYYT-M-BNNN  T is one of:
#  A/B - regular semester program
#  F - FT
#  D - DD
#  L - LP
#  W - PW
#  V - SV
#  S - DS
#  X - XT
# M is observing mode "(Q/C), could add P for PV"
scire = r"^(G[NS]?)-(20\d\d([A-Z]))-(Q|C|SV|QS|DD|LP|FT|DS|ENG|CAL)-(\d+)"

# This matches a program id
progre = r'(?:^%s$)|(?:^%s$)|(?:^%s$)' % (calengre, scire, calengre_old)

# This matches an observation id with the project id and obsnum as groups
obsre = r'((?:^%s)|(?:^%s)|(?:^%s))-(?P<obsid>\d*)$' % (calengre, scire, calengre_old)

# Here are some lists of defined detector settings
gemini_gain_settings = ('high', 'low', 'standard')
gemini_readspeed_settings = ('fast', 'slow')
gemini_welldepth_settings = ('Shallow', 'Deep', 'Invalid')
gemini_readmode_settings = ('Classic',
                            'NodAndShuffle',
                            'Faint',
                            'Faint_Object',
                            'Faint_Objects',
                            'Very_Faint_Objects',
                            'Medium',
                            'Medium_Object',
                            'Bright',
                            'Bright_Object',
                            'Bright_Objects',
                            'Very_Bright_Objects',
                            'Low_Background',
                            'Medium_Background',
                            'High_Background')

gemini_telescopes = {
    'gemini-north': 'Gemini-North',
    'gemini-south': 'Gemini-South'
}


# ------------------------------------------------------------------------------
def gemini_telescope(string) -> str:
    """
    If the string argument matches a gemini telescope name, then returns the
    "official" (ie same as in the fits headers) name of the telesope. Otherwise
    returns None.

    Parameters
    ----------
    string : <str>
        A string representing a Gemini telescope.

    Return
    ------
    <str> or <NoneType>
        The "official" name of the telesope, as found in Gemini
        fits headers.

    """
    try:
        return gemini_telescopes.get(string.lower())
    except AttributeError:
        return None


# A utility function for matching instrument names
hqcre = re.compile(r'^[Hh][Oo][Kk][Uu][Pp]([Aa])+(\+)*[Qq][Uu][Ii][Rr][Cc]$')
gemini_instrument_dict = {
    'niri': 'NIRI',
    'nifs': 'NIFS',
    'gmos-n': 'GMOS-N',
    'gmos-s': 'GMOS-S',
    'michelle': 'michelle',
    'gnirs': 'GNIRS',
    'ghost': 'GHOST',
    'phoenix': 'PHOENIX',
    'texes': 'TEXES',
    'trecs': 'TReCS',
    'nici': 'NICI',
    'igrins': 'IGRINS',
    'gsaoi': 'GSAOI',
    'oscir': 'OSCIR',
    'f2': 'F2',
    'gpi': 'GPI',
    'abu': 'ABU',
    'bhros': 'bHROS',
    'hrwfs': 'hrwfs',
    'flamingos': 'FLAMINGOS',
    'cirpass': 'CIRPASS',
    'graces': 'GRACES',
    'alopeke': 'ALOPEKE',
    'zorro': 'ZORRO',
    'maroon-x': 'MAROON-X'
}


def gemini_instrument(string: str, gmos: bool = False, other: bool = False) -> str:
    """
    If the string argument matches a gemini instrument name, then returns the
    "official" (ie same as in the fits headers) name of the instrument. Otherwise
    returns None.

    If the gmos argument is True, this also recognises GMOS as a valid instrument
    name. If the 'other' is True, it will pass through unknown instrument
    names that don't look like an official instrument rather than return None

    Parameters
    ----------
    string : <str>
        A string representing a Gemini instrument name.

    gmos : <bool>
        If True, this recognises 'GMOS' as a valid instrument.
        Default is False.

    other: <bool>
        If True, it will pass through unknown instrument names that don't look
        like an official instrument.
        Default is False.

    Return
    ------
    retary: <str> or <NoneType>
        The "official" name of the instrument, as found in Gemini
        fits headers.

    """
    retary = None

    if other:
        retary = string

    try:
        retary = gemini_instrument_dict[string.lower()]
    except KeyError:
        if string:
            if hqcre.match(string):
                retary = 'Hokupaa+QUIRC'
            elif gmos and string.lower() == 'gmos':
                retary = 'GMOS'

    return retary


def get_fake_ut(transit: str = "14:00:00"):
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


def gemini_date(string: str, as_datetime: bool = False, offset: timedelta = ZERO_OFFSET) \
        -> Union[datetime.datetime, str, None]:
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
    dt_to_text_short = lambda x: x.strftime('%Y%m%dT%H%M%S') + suffix

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
        # Example (HST): 2020-04-15 0:00 -10 hrs = 2020-04-14 2pm + 10 hrs = 2020-04-15 0:00
        # Example (CL): 2020-04-15 0:00 -10 hrs = 2020-04-14 2pm + 4 hrs = 2020-04-14 18:00
        # offset (HST) = -10 + 10 = 0
        # offset (CL) = -10 + 4 = -6
        try:
            dt = dateutil.parser.parse(string) + offset
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text(dt) if not as_datetime else dt
        except ValueError:
            pass

    if len(string) >= 14 and 'T' in string and ':' in string and '=' not in string:
        # Parse an ISO style datestring, so 2019-12-10T11:22:33.444444
        try:
            # TODO this is dateutil bug #786, so for now we truncate to 6 digits
            if '.' in string:
                lastdot = string.rindex('.')
                if len(string) - lastdot > 6:
                    string = string[:lastdot - len(string)]
            # TODO end of workaround
            dt = dateutil.parser.isoparse("%sZ" % string) + offset
            # strip back out time zone as the rest of the code does not support it
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text_full(dt) if not as_datetime else dt
        except ValueError as ve:
            pass

    if len(string) >= 14 and 'T' in string and ':' not in string and '=' not in string and '-' not in string:
        # Parse an compressed style datestring, so 20191210T112233
        try:
            dt = dateutil.parser.isoparse("%s-%s-%sT%s:%s:%sZ" % (string[0:4], string[4:6], string[6:8],
                                                                  string[9:11], string[11:13], string[13:15])) # + offset
            # strip back out time zone as the rest of the code does not support it
            dt = dt.replace(tzinfo=None)
            if DATE_LIMIT_LOW <= dt < DATE_LIMIT_HIGH:
                return dt_to_text_full(dt) if not as_datetime else dt
        except ValueError as ve:
            pass

    return '' if not as_datetime else None


def ratodeg(string: str) -> float:
    """
    A utility function that recognises an RA: HH:MM:SS.sss
    Or a decimal degrees RA value
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    try:
        return float(string)
    except:
        # ok, fall back to smart parsing
        pass
    try:
        return Angle("%s %s" % (string, "hours")).degree
    except:
        # unparseable
        pass
    return None


# deprecated, used by ratodeg_old
racre = re.compile(r'^([012]\d):([012345]\d):([012345]\d)(\.?\d*)$')


# deprecated
def ratodeg_old(string: str) -> float:
    """
    A utility function that recognises an RA: HH:MM:SS.sss
    Or a decimal degrees RA value
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    re_match = racre.match(string)
    if re_match is None:
        # Not HH:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            if value <= 360.0 and value >= 0.0:
                return value
        except:
            return None
        return None
    hours = float(re_match.group(1))
    mins = float(re_match.group(2))
    secs = float(re_match.group(3))
    frac = re_match.group(4)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs / 60.0
    hours += mins / 60.0

    degs = 15.0 * hours

    return degs


def dectodeg(string: str) -> float:
    """
    A utility function that recognises a Dec: [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    try:
        value = float(string)
        if value >= -90.0 and value <= 90.0:
            return value
    except:
        pass
    try:
        a = Angle("%s %s" % (string, "degrees"))
        if hasattr(a, "degrees"):
            return a.degrees
        else:
            return a.value
    except:
        # unparseable
        return None


# deprecated, used by dectodeg_old
deccre = re.compile(r'^([+-]?)(\d\d):([012345]\d):([012345]\d)(\.?\d*)$')


# deprecated
def dectodeg_old(string: str) -> float:
    """
    A utility function that recognises a Dec: [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    re_match = deccre.match(string)
    if re_match is None:
        # Not DD:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            if value >= -90.0 and value <= 90.0:
                return value
        except:
            return None
        return None
    sign = 1
    if re_match.group(1) == '-':
        sign = -1

    degs = float(re_match.group(2))
    mins = float(re_match.group(3))
    secs = float(re_match.group(4))
    frac = re_match.group(5)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs / 60.0
    degs += mins / 60.0

    degs *= sign

    return degs


def degtora(decimal: float) -> str:
    """
    Convert decimal degrees to RA HH:MM:SS.ss string
    """
    decimal /= 15.0
    hours = int(decimal)
    decimal -= hours

    decimal *= 60.0
    minutes = int(decimal)
    decimal -= minutes

    decimal *= 60.0
    seconds = decimal

    return "%02d:%02d:%05.2f" % (hours, minutes, seconds)


def degtodec(decimal: float) -> str:
    """
    Convert decimal degrees to Dec +-DD:MM:SS.ss string
    """
    sign = '+' if decimal >= 0.0 else '-'
    decimal = abs(decimal)
    degrees = int(decimal)
    decimal -= degrees

    decimal *= 60.0
    minutes = int(decimal)
    decimal -= minutes

    decimal *= 60.0
    seconds = decimal

    return "%s%02d:%02d:%05.2f" % (sign, degrees, minutes, seconds)


dmscre = re.compile(r'^([+-]?)(\d*):([012345]\d):([012345]\d)(\.?\d*)$')


def dmstodeg(string: str) -> float:
    """
    A utility function that recognises a generic [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    string = string.replace(' ', '')
    re_match = dmscre.match(string)
    if re_match is None:
        # Not DD:MM:SS. Maybe it's decimal degrees already
        try:
            value = float(string)
            return value
        except:
            return None
        return None
    sign = 1
    if re_match.group(1) == '-':
        sign = -1
    degs = float(re_match.group(2))
    mins = float(re_match.group(3))
    secs = float(re_match.group(4))
    frac = re_match.group(5)
    if frac:
        frac = float(frac)
    else:
        frac = 0.0

    secs += frac
    mins += secs / 60.0
    degs += mins / 60.0

    degs *= sign

    return degs


srcre = re.compile(r"([\d.]+)\s*(d|D|degs|Degs)?")


def srtodeg(string: str) -> float:
    """
    Converts a Search Radius in arcseconds to decimal degrees.

    Assume arcseconds unless the string ends with 'd' or 'degs'

    Parameters
    ----------
    string : <str>
        A string representing a search radius in arcseconds

    Return
    ------
    value: <float> or <NoneType>
        A search radius in decimal degrees, None if invalid.

    """
    match = srcre.match(string)
    try:
        value = float(match.group(1))
        degs = match.group(2)
    except:
        return None

    if degs is None:
        # Value is in arcseconds, convert to degrees
        value /= 3600.0

    return value


def gemini_daterange(string: str, as_datetime: bool = False, offset: timedelta = ZERO_OFFSET) \
        -> Union[datetime.datetime, str, None]:
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


procmode_codes = ('sq', 'ql', 'qa')


def gemini_procmode(string: str) -> str:
    """
    A utility function for matching Gemini Processed Mode.

    If the string argument matches a gemini Processed Mode Code then we return the
    code else return None

    Parameters
    ----------
    string : <str>
        Name of a processed mode code.

    Return
    ------
    string : <str> or <NoneType>
        The name of the processed mode code or None.

    """
    return string if string in procmode_codes else None


obs_types = ('DARK', 'ARC', 'FLAT', 'BIAS', 'OBJECT', 'PINHOLE', 'RONCHI', 'CAL', 'FRINGE', 'MASK', 'STANDARD',
             'SLITILLUM', 'BPM')


def gemini_observation_type(string: str) -> str:
    """
    A utility function for matching Gemini ObsTypes.

    We add the unofficial values PINHOLE for GNIRS pinhole mask observations
    and RONCHI for NIFS Ronchi mask observations.

    If the string argument matches a gemini ObsType then we return the
    observation_type else return None

    Parameters
    ----------
    string : <str>
        Name of a gemini observation type as returned by AstroData
        descriptor, observation_type().

    Return
    ------
    string : <str> or <NoneType>
        The name of the observation type or None.

    """
    return string if string in obs_types else None


obs_classes = ('dayCal', 'partnerCal', 'acqCal', 'acq', 'science', 'progCal')


def gemini_observation_class(string: str) -> str:
    """
    A utility function matching Gemini ObsClasses.

    If the string argument matches a gemini ObsClass then we return the
    observation_class, else return None

    Parameters
    ----------
    string : <str>
        Name of a gemini observation class as returned by AstroData
        descriptor, observation_class().

    Return
    ------
    string : <str> or <NoneType>
        The name of the observation class or None.

    """
    return string if string in obs_classes else None


reduction_states = ('RAW', 'PREPARED', 'PROCESSED_FLAT', 'PROCESSED_BIAS',
                    'PROCESSED_FRINGE', 'PROCESSED_ARC', 'PROCESSED_DARK',
                    'PROCESSED_TELLURIC', 'PROCESSED_SCIENCE', 'PROCESSED_STANDARD',
                    'PROCESSED_SLITILLUM', 'PROCESSED_BPM', 'PROCESSED_UNKNOWN')


def gemini_reduction_state(string: str) -> str:
    """
    A utility function matching Gemini reduction states.

    If the string argument matches a gemini reduction state then we return the
    reduction state else return None.

    Parameters
    ----------
    string : <str>
        Name of a reduction state as enumerated in 'reduction_states'.

    Return
    ------
    string : <str> or <NoneType>
        The name of reduction state or None.

    """
    return string if string in reduction_states else None


cal_types = (
    'bias', 'dark', 'flat', 'arc', 'processed_bias', 'processed_dark',
    'processed_flat', 'processed_arc', 'processed_fringe', 'pinhole_mask',
    'ronchi_mask', 'spectwilight', 'lampoff_flat', 'qh_flat', 'specphot',
    'photometric_standard', 'telluric_standard', 'domeflat', 'lampoff_domeflat',
    'mask', 'polarization_standard', 'astrometric_standard', 'polarization_flat',
    'processed_standard', 'processed_slitillum', 'slitillum', 'processed_bpm',
)


def gemini_caltype(string: str) -> str:
    """
    A utility function matching Gemini calibration types.
    If the string argument matches a gemini calibration type then we return
    the calibration type, otherwise we return None

    The list of calibration types is somewhat arbitrary, it's not coupled
    to the DHS or ODB, it's more or less defined by the Fits Storage project

    These should all be lower case so as to avoid conflict with gemini_observation_type

    Parameters
    ----------
    string : <str>
        Name of a calibration type.

    Return
    ------
    string : <str> or <NoneType>
        The name of calibration type or None.

    """
    return string if string in cal_types else None


gmos_gratings = ('MIRROR', 'B480', 'B600', 'R600', 'R400', 'R831', 'R150', 'B1200')


def gmos_gratingname(string: str) -> str:
    """
    A utility function matching a GMOS Grating name. This could be expanded to
    other instruments, but for many instruments the grating name is too ambiguous and
    could be confused with a filter or band (eg 'H'). Also most of the use cases
    for this are where gratings are swapped.

    This function doesn't match or return the component ID.

    If the string argument matches a grating, we return the official name for
    that grating.

    Parameters
    ----------
    string : <str>
        A grating name.

    Return
    ------
    string : <str> or <NoneType>
         A grating name or None.

    """
    return string if string in gmos_gratings else ''


gmosfpmaskcre = re.compile(r'^G[NS]?(20\d\d)[ABFDLWVSX](.)(\d\d\d)-(\d\d)$')
gmos_facility_plane_masks = (
    'NS2.0arcsec', 'IFU-R', 'IFU-B', 'focus_array_new', 'Imaging', '2.0arcsec',
    'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec', 'IFU-2', 'NS1.5arcsec',
    '0.75arcsec', '1.0arcsec', '0.5arcsec'
)


def gmos_focal_plane_mask(string: str) -> str:
    """
    A utility function matching gmos focal plane mask names. This could be expanded to
    other instruments. Most of the uses cases for this are for masks that are swapped.
    This function knows the names of the facility masks (long slits, NSlongslits and IFUs)
    Also it knows the form of the MOS mask names and will return a mosmask name if the string
    matches that format, even if that maskname does not actually exist

    If the string matches an focal_plane_mask, we return the focal_plane_mask.
    """

    if (string in gmos_facility_plane_masks) or gmosfpmaskcre.match(string):
        return string

    return None


fitsfilenamecre = re.compile(r'^([NS])(20\d\d)([01]\d[0123]\d)(S)(\d\d\d\d)([\d-]*)(\w*)(?P<fits>.fits)?$')
vfitsfilenamecre = re.compile(
    r'^(20)?(\d\d)(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d\d)_(\d+)(?P<fits>.fits)?$')


def gemini_fitsfilename(string: str) -> str:
    """
    A utility function matching Gemini data fits filenames
    If the string argument matches the format of a gemini
    data filename, with or without the .fits on the end, this
    function will return the filename, with the .fits on the end.

    If the string does not look like a filename, we return an empty string.
    """
    retval = ''
    m = fitsfilenamecre.match(string) or vfitsfilenamecre.match(string)
    if m:
        # Yes, but does it not have a .fits?
        if m.group('fits') == None:
            retval = "%s.fits" % string
        else:
            retval = string

    return retval


def gemini_binning(string: str) -> str:
    """
    A utility function that matches a binning string -
    for example 1x1, 2x2, 1x4
    """

    valid = '1248'
    a, sep, b = string.partition('x')

    return string if (a and b and (a in valid) and (b in valid)) else ''


def percentilestring(num: int, type: str) -> str:
    """
    A utility function that converts a numeric percentile
    number, and the site condition type, into a compact string,
    eg (20, 'IQ') -> IQ20. Maps 100 onto 'Any' and gives
    'Unknown' if the num is None
    """

    if num is None:
        return 'Undefined'

    if num == 100:
        return type + "Any"

    return "%s%02d" % (type, num)


# The Gemini Data Label Class

# This re matches program_id-obsum-dlnum - ie a datalabel,
# With 3 groups - program_id, obsnum, dlnum
# This also allows for an optional -blah on the end (processed biases etc)

dlcre = re.compile(r'^(?P<progid>(?:%s)|(?:%s)|(?:%s))-(?P<obsid>\d*)-(?P<dlid>\d*)(?:-(?P<extn>[-\w]*))?$' % (calengre, scire, calengre_old))
# dlcre = re.compile(r'^((?:%s)|(?:%s)|(?:%s))-(\d*)-(\d*)(?:-([-\w]*))?$' %\
#                    (r'^G[NS]?-20\d\d[ABFDLWVSX]-((?:CAL)|(?:ENG))-(?:[A-Za-z0-9_]+-)?\d+',
#                     r"^(?:G[NS]?)-(?:20\d\d([A-Z]))-(?:Q|C|SV|QS|DD|LP|FT|DS|ENG|CAL)-(?:\d+)",
#                     r'G[NS]-((?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'))


class GeminiDataLabel:
    """
    Construct a GeminiDataLabel from the given datalabel string.

    This will parse the passed datalabel and fill in the various fields
    with values inferred from the datalabel.

    dl: str
        datalabel to use
    """

    datalabel = ''
    projectid = ''
    observation_id = ''
    obsnum = ''
    dlnum = ''
    extension = ''
    project = ''

    def __init__(self, dl: str):
        """
        Construct a GeminiDataLabel from the given datalabel string.

        This will parse the passed datalabel and fill in the various fields
        with values inferred from the datalabel.

        dl: str
            datalabel to use
        """
        # Clean up datalabel if it has space padding
        if dl is not None and isinstance(dl, str):
            dl = dl.strip()

        self.datalabel = dl              #: datalabel as a string
        self.projectid = ''              #: project id portion
        self.project = None              #: :class:`~gemini_obs_db.utils.gemini_metadata_utils.GeminiProgram` for the given project id
        self.observation_id = ''         #: observaiton id portion
        self.obsnum = ''                 #: observation number
        self.dlnum = ''                  #: datalabel number
        self.extension = ''              #: extension number, if any
        self.datalabel_noextension = ''  #: datalabel without the extension number suffix
        self.valid = False               #: True if datalabel is in the correct format
        if self.datalabel:
            self.parse()

    def parse(self):
        """
        Infer the other fields for this GeminiDataLabel based on the
        text datalabel.
        """
        dlm = dlcre.match(self.datalabel)
        if dlm:
            self.projectid = dlm.group('progid')
            self.obsnum = dlm.group('obsid')
            self.dlnum = dlm.group('dlid')
            self.extension = dlm.group('extn')
            self.project = GeminiProgram(self.projectid)
            self.observation_id = '%s-%s' % (self.projectid, self.obsnum)
            self.datalabel_noextension = '%s-%s-%s' % (self.projectid, self.obsnum, self.dlnum)
            self.valid = True
        else:
            # Match failed - Null the datalabel field
            self.datalabel = ''
            self.valid = False


class GeminiObservation:
    """
    The GeminiObservation class parses an observation ID

    Simply instantiate the class with an observation id string
    then reference the following data members:

    * observation_id: The observation ID provided. If the class cannot
                     make sense of the string passed in, this field will
                     be empty
    * project: A GeminiProgram object for the project this is part of
    * obsnum: The observation numer within the project

    Parameters
    ----------
    observation_id : str
        ObservationID from which to parse the information
    """
    observation_id = ''
    program = ''
    obsnum = ''

    def __init__(self, observation_id):
        # Clean up value if it has space padding
        if observation_id is not None and isinstance(observation_id, str):
            observation_id = observation_id.strip()

        if observation_id:
            match = re.match(obsre, observation_id)
            if match:
                self.observation_id = observation_id
                self.program = GeminiProgram(match.group(1))
                self.obsnum = match.group('obsid')
                self.valid = True
            else:
                self.observation_id = ''
                self.project = ''
                self.obsnum = ''
                self.valid = False
        else:
            self.observation_id = ''
            self.valid = False


class GeminiProgram:
    """
    The GeminiProgram class parses a Gemini Program ID and provides
    various useful information deduced from it.

    Simply instantiate the class with a program ID string, then
    reference the following data members:

    * program_id: The program ID passed in.
    * valid: a Boolean saying whether the program_id is a valid standard format
    * is_cal: a Boolean that is true if this is a CAL program
    * is_eng: a Boolean that is true if this is an ENG program
    * is_q: a Boolean that is true if this is a Queue program
    * is_c: a Boolean that is true if this is a Classical program
    * is_sv: a Boolean that is true if this is an SV (Science Verification) program
    * is_qs: a Boolean that is true if this is an QS (Quick Start) program
    * is_dd: a Boolean that is true if this is an DD (Directors Discretion) program
    * is_lp: a Boolean that is true if this is an LP (Large Program) program
    * is_ft: a Boolean that is true if this is an FT (Fast Turnaround) program
    * is_ds: a Boolean that is true if this is an DS (Demo Science) program

    This could be easily expanded to extract semester, hemisphere, program number etc
    if required.

    Parameters
    ----------
    program_id : str
        Gemini ProgramID to parse
    """
    program_id = None
    valid = None
    is_cal = False
    is_eng = False
    is_q = False
    is_c = False
    is_sv = False
    is_qs = False
    is_dd = False
    is_lp = False
    is_ft = False
    is_ds = False

    def __init__(self, program_id: str):
        # clean up any spaces
        if program_id is not None and isinstance(program_id, str):
            program_id = program_id.strip()

        self.program_id = program_id
        # Check for the CAL / ENG form
        ec_match_old = re.match(calengre_old + r'$', program_id)
        ec_match = re.match(calengre + r'$', program_id)
        sci_match = re.match(scire + r'$', program_id)
        if ec_match_old:
            # Valid eng / cal form
            self.valid = True
            self.is_eng = ec_match_old.group(1) == 'ENG'
            self.is_cal = ec_match_old.group(1) == 'CAL'
        elif ec_match:
            self.valid = True
            self.is_eng = ec_match.group(1) == 'ENG'
            self.is_cal = ec_match.group(1) == 'CAL'
        elif sci_match:
            # Valid science form
            self.valid = True
            self.is_q = sci_match.group(4) == 'Q'
            self.is_c = sci_match.group(4) == 'C'
            self.is_eng = sci_match.group(4) == 'ENG'
            self.is_cal = sci_match.group(4) == 'CAL'
            if program_id.startswith('G-'):
                self.is_sv = sci_match.group(3) == 'V'
                self.is_ft = sci_match.group(3) == 'F'
                self.is_ds = sci_match.group(3) == 'S'
            else:
                self.is_sv = sci_match.group(4) == 'SV'
                self.is_ft = sci_match.group(4) == 'FT'
                self.is_ds = sci_match.group(4) == 'DS'

            # If the program id is OLD style and program number contained leading zeros, strip them out of
            # the official program_id
            if sci_match.group(5)[0] == '0' and not program_id.startswith('G-'):
                prog_num = int(sci_match.group(5))
                self.program_id = "%s-%s-%s-%s" % (sci_match.group(1),
                                                   sci_match.group(2),
                                                   sci_match.group(4),
                                                   prog_num)

        else:
            # Not a valid format. Probably some kind of engineering test program
            # that someone just made up.
            self.valid = False
            self.is_eng = True


def get_date_offset() -> timedelta:
    """
    This function is used to add set offsets to the dates. The aim is to get the
    "current date" adjusting for the local time, taking into account the different
    sites where Gemini is based.

    Returns
    -------
    timedelta
        The `timedelta` to use for this application/server.
    """
    if db_config.use_utc:
        return ZERO_OFFSET

    # Calculate the proper offset to add to the date
    # We consider the night boundary to be 14:00 local time
    # This is midnight UTC in Hawaii, completely arbitrary in Chile
    zone = time.altzone if time.daylight else time.timezone
    # print datetime.timedelta(hours=16)
    # print datetime.timedelta(seconds=zone)
    # print ONEDAY_OFFSET

    # return datetime.timedelta(hours=16) + datetime.timedelta(seconds=zone) - ONEDAY_OFFSET
    # I think this interacted with the Chile today changes to cause the missing starts in Hawaii for summary
    return datetime.timedelta(hours=14) + datetime.timedelta(seconds=zone) - ONEDAY_OFFSET


def get_time_period(start: str, end: str = None, as_date: bool = False) \
        -> Union[Tuple[date, date], Tuple[datetime.datetime, datetime.datetime]]:
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
        If True, make return type `date` only, else return full `datetime` objects, defaults to False

    Returns
    -------
    tuple
        A tuple of `date` or `datetime` with the resulting parsed values, defaults to False
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


def gemini_time_period_from_range(rng: str, as_date: bool = False) \
        -> Union[Tuple[date, date], Tuple[datetime.datetime, datetime.datetime]]:
    """
    Get a time period from a passed in string representation

    Parameters
    ----------
    rng : str
        YYYYMMDD-YYYYMMDD style range
    as_date : bool
        If True, return tuple of `date`, else tuple of `datetime`, defaults to False

    Returns
    -------
    `tuple` of `datetime` or `tuple` of `date`
        Start and stop time of the period as `date` or `datetime` per `as_date`
    """
    a, _, b = gemini_daterange(rng).partition('-')
    return get_time_period(a, b, as_date)


def gemini_semester(dt: date) -> str:
    """
    Return the semester name that contains date

    Parameters
    ----------
    date : date
        The date to check for the owning semester.

    Returns
    -------
    str
        Semester code containing the provided date.
    """
    if dt.month >= 2 and dt.month <= 7:
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
