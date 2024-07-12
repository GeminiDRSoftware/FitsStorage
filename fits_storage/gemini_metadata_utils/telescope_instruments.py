import re


# Here are some lists of defined detector settings
gemini_gain_settings = ('high', 'low', 'standard')
gemini_readspeed_settings = ('fast', 'medium', 'slow',
                             # And these GHOST ones...
                             'standard',
                             'red:fast,blue:fast',
                             'red:fast,blue:medium',
                             'red:fast,blue:slow',
                             'red:medium,blue:fast',
                             'red:medium,blue:medium',
                             'red:medium,blue:slow',
                             'red:slow,blue:fast',
                             'red:slow,blue:medium',
                             'red:slow,blue:slow')

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



# sortkey_regex_dict:
# These regular expessions are used by the queues to determine how to sort (
# ie prioritize) files when despooling the queues. The regexes should provide
# two named groups - date (YYYYMMDD) and optional num (serial number). The
# regexes are keys in a dict, where the value is string representing a higher
# level via reverse alpha sort - ie files matching regexes with value 'z' are
# considered highest priority, and those matching regexes with value 'x' are
# next in priority. This allows for example to prioritize science files over
# site monitoring data. The regexes should be compiled here for efficiency.

_standard_filename_re = re.compile(r'[NS](?P<date>\d{8})\w(?P<num>\d+).*')
_igrins_filename_re = re.compile(r'SDC[SHK]_(?P<date>\d{8})_(?P<num>\d+).*')
_skycam_filename_re = re.compile(r'img_(?P<date>\d{8})_(?P<num>\w+).*')
_obslog_filename_re = re.compile(r'(?P<date>\d{8})_(?P<num>.*)_obslog.txt')
sortkey_regex_dict = {_standard_filename_re: 'z',
                      _igrins_filename_re: 'z',
                      _obslog_filename_re: 'x',
                      _skycam_filename_re: 'y'}


def gemini_telescope(string):
    """
    If the string argument matches a gemini telescope name, then return the
    "official" form of the name of the telescope. Otherwise, return None.
    This basically fixes capitalization and '-'/'_' errors.

    Parameters
    ----------
    string : <str>
        A string representing a Gemini telescope, eg 'geminiNorth'

    Return
    ------
    <str> or <NoneType>
        The "official" name of the telescope, as found in Gemini
        fits headers, eg 'Gemini-North'.

    """
    if not isinstance(string, str):
        return None

    gemini_telescopes = {
        'gemininorth': 'Gemini-North',
        'geminisouth': 'Gemini-South'
    }

    try:
        s = string.lower().replace('-', '').replace('_', '')
        return gemini_telescopes.get(s)
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
    'igrins-2': 'IGRINS-2',
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


def gemini_instrument(string, gmos=False, other=False):
    """
    If the string argument matches a gemini instrument name, then returns the
    "official" (ie same as in the fits headers) name of the instrument.
    Otherwise, returns None.

    If the gmos argument is True, this recognises 'GMOS' as a valid instrument
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


# These are the new (2024) official processing modes. As used in the
# FitsStorage reduction table, and thus archive. Note, the order is significant
# here as this is used directly to create the enumerated SQL type and sorting
# by that type in postgres will respect the order in the enum, which is the
# order here. We use this sort to return calibrations "processed first", so the
# order should generally be from least to most processed.
gemini_processing_modes = ('Failed', 'Raw', 'Quick-Look', 'Science-Quality')


def gemini_processing_mode(string: str) -> str:
    """
    A utility function for matching Gemini Processed Mode.

    If the string argument matches a gemini Processed Mode Code then we return
    the code else return None

    Parameters
    ----------
    string : <str>
        Name of a processed mode code.

    Return
    ------
    string : <str> or <NoneType>
        The name of the processed mode code or None.

    """
    return string if string in gemini_processing_modes else None


obs_types = ('DARK', 'ARC', 'FLAT', 'BIAS', 'OBJECT', 'PINHOLE', 'RONCHI',
             'CAL', 'FRINGE', 'MASK', 'STANDARD', 'SLITILLUM', 'BPM')


def gemini_observation_type(string):
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


def gemini_observation_class(string):
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
                    'PROCESSED_TELLURIC', 'PROCESSED_SCIENCE', 'PROCESSED_BPM',
                    'PROCESSED_STANDARD', 'PROCESSED_SLITILLUM',
                    'PROCESSED_PINHOLE', 'PROCESSED_UNKNOWN')


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
    'processed_flat', 'processed_arc', 'processed_fringe', 'pinhole',
    'processed_pinhole', 'ronchi_mask', 'spectwilight', 'lampoff_flat',
    'qh_flat', 'specphot', 'photometric_standard', 'telluric_standard',
    'domeflat', 'lampoff_domeflat', 'mask', 'polarization_standard',
    'astrometric_standard', 'polarization_flat', 'processed_standard',
    'processed_slitillum', 'slitillum', 'processed_bpm',
)


def gemini_caltype(string: str) -> str:
    """
    A utility function matching Gemini calibration types.
    If the string argument matches a gemini calibration type then we return
    the calibration type, otherwise we return None

    The list of calibration types is somewhat arbitrary, it's not coupled
    to the DHS or ODB, it's more or less defined by the Fits Storage project

    These must all be lower case to avoid conflict with gemini_observation_type

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


def gmos_gratingname(string):
    """
    A utility function matching a GMOS Grating name. This could be expanded to
    other instruments, but for many instruments the grating name is too
    ambiguous and could be confused with a filter or band (eg 'H'). Also, most
    of the use cases for this are where gratings are swapped.

    This function doesn't match or return the component ID.

    If the string argument matches a grating, we return the official name for
    that grating. Otherwise, we return None

    Parameters
    ----------
    string : <str>
        A grating name.

    Return
    ------
    string : <str> or <NoneType>
         A grating name or None.

    """
    gmos_gratings = ('MIRROR', 'B480', 'B600', 'R600', 'R400', 'R831',
                     'R150', 'B1200')

    return string if string in gmos_gratings else None


def gmos_dispersion(string):
    """
    Returns an estimate of the gmos dispersion in um/pix given a grating name.
    Note, this is not exact, it is an approximation that is used primarily for
    estimating a sensible central wavelength tolerance on calibration matches.
    """
    grating = gmos_gratingname(string)
    if grating is None or grating == 'MIRROR':
        return None
    lmm = float(grating.strip('BR'))
    dispersion = 0.03/lmm
    return dispersion


def gmos_focal_plane_mask(string):
    """
    A utility function matching gmos focal plane mask names. This could be
    expanded to other instruments. Most of the uses cases for this are for
    masks that are swapped. This function knows the names of the facility masks
    (long slits, NSlongslits and IFUs). Also, it knows the form of the MOS mask
    names and will return a mosmask name if the string matches that format,
    even if that maskname does not actually exist

    If the string matches a focal_plane_mask, we return the focal_plane_mask.
    """

    gmosfpmaskre_old = r'^G[NS]?(20\d\d)[ABFDLWVSX](.)(\d\d\d)-(\d\d)$'
    gmosfpmaskre_new = r'^G(20\d\d)[AB](\d\d\d\d)[CDFLQSV]-(\d\d)$'
    gmosfpmaskcre = re.compile("%s|%s" % (gmosfpmaskre_old, gmosfpmaskre_new))

    gmos_facility_plane_masks = (
        'NS2.0arcsec', 'IFU-R', 'IFU-B', 'focus_array_new', 'Imaging',
        '2.0arcsec', 'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec',
        'IFU-2', 'NS1.5arcsec', '0.75arcsec', '1.0arcsec', '0.5arcsec')

    if (string in gmos_facility_plane_masks) or gmosfpmaskcre.match(string):
        return string

    return None


def gemini_fitsfilename(string):
    """
    A utility function matching Gemini data fits filenames. If the string
    argument matches the format of a gemini data filename, with or without the
    .fits on the end, and with or without a trailiing .bz2, this function will
    return the filename, with the .fits on the end (but no .bz2)

    If the string does not look like a filename, we return an empty string.
    """

    fitsfilenamecre = re.compile(
        r'^([NS])(20\d\d)([01]\d[0123]\d)(S)(\d\d\d\d)([\d-]*)(\w*)'
        r'(?P<fits>.fits)?$')
    vfitsfilenamecre = re.compile(
        r'^(20)?(\d\d)(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d\d)'
        r'_(\d+)(?P<fits>.fits)?$')

    string = string.removesuffix('.bz2')

    retval = ''
    m = fitsfilenamecre.match(string) or vfitsfilenamecre.match(string)
    if m:
        # Yes, but does it not have a .fits?
        if m.group('fits') is None:
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


def percentilestring(num, type) -> str:
    """
    A utility function that converts a numeric percentile
    number, and the site condition type, into a compact string,
    eg (20, 'IQ') -> IQ20. Maps 100 onto 'Any' and gives
    'Undefined' if the num is None
    """

    if num is None:
        return 'Undefined'

    if num == 100:
        return type + "Any"

    return "%s%02d" % (type, num)


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
