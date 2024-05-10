from astropy.coordinates import Angle

import re


def ratodeg(string):
    """
    A utility function that recognises an RA: HH:MM:SS.sss
    Or a decimal degrees RA value
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    try:
        return float(string)
    except ValueError:
        # ok, fall back to smart parsing
        pass
    try:
        return Angle("%s %s" % (string, "hours")).degree
    except:
        # unparseable
        pass
    return None


def dectodeg(string):
    """
    A utility function that recognises a Dec: [+-]DD:MM:SS.sss
    Returns a float in decimal degrees if it is valid, None otherwise
    """
    try:
        value = float(string)
        if -90.0 <= value <= 90.0:
            return value
    except ValueError:
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


def dmstodeg(string):
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
        except ValueError:
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


def srtodeg(string):
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
    except ValueError:
        return None

    if degs is None:
        # Value is in arcseconds, convert to degrees
        value /= 3600.0

    return value
