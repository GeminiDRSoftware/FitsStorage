"""
This module deals with the 'selection' concept.
"""

import re
import math
import urllib.parse
import urllib.error
import dateutil.parser

from sqlalchemy import or_, and_, func

import fits_storage.gemini_metadata_utils as gmu

# TODO - get rid of this link into the GPI table
from fits_storage.cal.orm.gpi import Gpi

from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File
from fits_storage.server.orm.program import Program
from fits_storage.server.orm.publication import Publication, ProgramPublication

from fits_storage.config import get_config
fsc = get_config()

# The getselection() function converts a list of elements from the URL
# into a selection dictionary. We loop through the elements in the URL
# and test each one against some criteria to decide how to handle it. Common
# tests are grouped into different types and described by these dictionaries
# up-front.

# The key here is the selection key, and the value is either a callable or a
# tuple. For callables, if the callable returns a value other than None, that
# value is set as the value of that selection key. For tuples, if the element
# is in the tuple, it is set as the value of that selection key.
getselection_tests = {
    'telescope': gmu.gemini_telescope,
    'filename': gmu.gemini_fitsfilename,
    'observation_type': gmu.gemini_observation_type,
    'observation_class': gmu.gemini_observation_class,
    'caltype': gmu.gemini_caltype,
    'processing': gmu.gemini_processing_mode,
    'reduction': gmu.gemini_reduction_state,
    'disperser': gmu.gmos_gratingname,
    'focal_plane_mask': gmu.gmos_focal_plane_mask,
    'binning': gmu.gemini_binning,
    'gain': gmu.gemini_gain_settings,
    'readspeed': gmu.gemini_readspeed_settings,
    'welldepth': gmu.gemini_welldepth_settings,
    'readmode': gmu.gemini_readmode_settings,
    'inst': (lambda x: gmu.gemini_instrument(x, gmos=True))
}

# Some other elements in the URL are of the key=value form - these values are
# set without further check. They key in this dictionary is the keyword in the
# URL element. The value is the key in the selection dictionary.

getselection_key_value = {
    'filename': 'filename',
    'disperser': 'disperser',
    'camera': 'camera',
    'mask': 'focal_plane_mask',
    'pupil_mask': 'pupil_mask',
    'filter': 'filter', 'Filter': 'filter',
    'az': 'az', 'Az': 'az',
    'azimuth': 'az', 'Azimuth': 'az',
    'el': 'el', 'El': 'el',
    'elevation': 'el', 'Elevation': 'el',
    'ra': 'ra', 'RA': 'ra',
    'dec': 'dec', 'Dec': 'dec',
    'sr': 'sr', 'SR': 'sr',
    'crpa': 'crpa', 'CRPA': 'crpa',
    'filepre': 'filepre',
    'cenwlen': 'cenwlen',
    'exposure_time': 'exposure_time',
    'coadds': 'coadds',
    'publication': 'publication',
    'PIname': 'PIname',
    'ProgramText': 'ProgramText',
    'raw_cc': 'raw_cc',
    'raw_iq': 'raw_iq',
    'ephemeris_target': 'ephemeris_target',
    'gain': 'gain',
    'readspeed': 'readspeed',
    'date': 'date',
    'daterange': 'daterange',
    'night': 'night',
    'nightrange': 'nightrange',
    'entrytimedaterange': 'entrytimedaterange'
    }

# Some elements of the URL entries set themselves as the value for a
# keyword in the selection dictionary.
getselection_simple_associations = {
    'warnings': 'caloption',
    'missing': 'caloption',
    'requires': 'caloption',
    'takenow': 'caloption',
    'Pass': 'qa_state',
    'Usable': 'qa_state',
    'Fail': 'qa_state',
    'Win': 'qa_state',
    'NotFail': 'qa_state',
    'Lucky': 'qa_state',
    'AnyQA': 'qa_state',
    'CHECK': 'qa_state',
    'UndefinedQA': 'qa_state',
    'AO': 'ao',
    'NOTAO': 'ao',
    }

# Some elements set a certain selection entry to boolean value...
getselection_booleans = {
    'imaging': ('spectroscopy', False),
    'spectroscopy': ('spectroscopy', True),
    'present': ('present', True), 'Present': ('present', True),
    'notpresent': ('present', False), 'NotPresent': ('present', False),
    'canonical': ('canonical', True), 'Canonical': ('canonical', True),
    'notcanonical': ('canonical', False), 'NotCanonical': ('canonical', False),
    'engineering': ('engineering', True),
    'notengineering': ('engineering', False),
    'science_verification': ('science_verification', True),
    'notscience_verification': ('science_verification', False),
    'calprog': ('calprog', True),
    'notcalprog': ('calprog', False),
    'site_monitoring': ('site_monitoring', True),
    'not_site_monitoring': ('site_monitoring', False),
    'photstandard': ('photstandard', True),
    'mdgood': ('mdready', True),
    'mdbad': ('mdready', False),
    'gpi_astrometric_standard': ('gpi_astrometric_standard', True),

    # this is basically a dummy value for the search form defaults
    'includeengineering': ('engineering', 'Include'),
    }

getselection_detector_roi = {
    'fullframe': 'Full Frame',
    'centralstamp': 'Central Stamp',
    'centralspectrum': 'Central Spectrum',
    'central768': 'Central768',
    'central512': 'Central512',
    'central256': 'Central256',
    'custom': 'Custom'
    }


def getselection(things):
    """
    This takes a list of things from the URL, and returns a selection dict.
    We disregard all but the most specific of a project id, observation id
    or datalabel.

    If a raw date, eg /YYYYMMDD is specified in the URL, whether it is treated
    as a UTC date or a "night" date is configuration dependent.
    """
    selection = {}
    for thing in things:
        for key in getselection_tests.keys():
            if callable(getselection_tests[key]):
                r = getselection_tests[key](thing)
                if r:
                    selection[key] = r
                    break
            else:
                if thing in getselection_tests[key]:
                    selection[key] = thing
                    break

        else:
            key, sep, value = thing.partition('=')
            if sep != '=':
                value = thing

            if sep == '=' and key in getselection_key_value:
                selection[getselection_key_value[key]] = value
            elif sep == '=' and key == 'cols':
                selection['cols'] = value
            elif thing in getselection_booleans:
                kw, val = getselection_booleans[thing]
                selection[kw] = val
            elif thing in getselection_simple_associations:
                selection[getselection_simple_associations[thing]] = thing
            elif gmu.GeminiProgram(thing).valid:
                selection['program_id'] = gmu.GeminiProgram(thing).program_id
            elif key == 'progid':
                if value is not None and isinstance(value, str):
                    value = value.strip()
                if gmu.GeminiDataLabel(value).valid:
                    selection['data_label'] = value
                elif gmu.GeminiObservation(value).valid:
                    selection['observation_id'] = value
                else:
                    selection['program_id'] = value
            elif gmu.GeminiObservation(thing).observation_id or key == 'obsid':
                selection['observation_id'] = value.strip()
            elif gmu.GeminiDataLabel(thing).datalabel or key == 'datalabel':
                selection['data_label'] = value.strip()
            elif thing in {'LGS', 'NGS'}:
                selection['lgs'] = thing
                # Make LGS / NGS selection imply AO selection
                selection['ao'] = 'AO'
            elif thing in {'Raw', 'Quick-Look', 'Science-Quality'}:
                selection['processing'] = thing
            elif thing.lower() in getselection_detector_roi:
                selection['detector_roi'] = \
                    getselection_detector_roi[thing.lower()]
            elif thing.lower() == 'preimage':
                selection['pre_image'] = True
            elif thing.lower() == 'twilight':
                selection['twilight'] = True
            elif thing.lower() == 'nottwilight':
                selection['twilight'] = False
            elif (len(thing) < 14) and (thing[:4] in {'N200', 'N201', 'N202',
                                                      'S200', 'S201', 'S202'}):
                # Good through 2029, don't match full filenames :-)
                selection['filepre'] = thing
            elif key in {'object', 'Object'}:
                selection['object'] = value
            elif thing in {'LS', 'MOS', 'IFS'}:
                selection['mode'] = thing
                selection['spectroscopy'] = True
            elif thing.lower() == 'standard':
                selection['standard'] = True
            elif gmu.gemini_date(thing):
                # Handle raw date strings in the URL
                if fsc.is_archive:
                    # On the archive, handle raw dates as UTC
                    selection['date'] = thing
                else:
                    # On the summit servers, handle raw dates as Nights
                    selection['night'] = thing
            elif gmu.gemini_daterange(thing):
                if fsc.is_archive:
                    # On the archive, handle raw dates as UTC
                    selection['daterange'] = thing
                else:
                    # On the summit servers, handle raw dates as Nights
                    selection['nightrange'] = thing
            else:
                if 'notrecognised' in selection:
                    selection['notrecognised'] += " "+thing
                else:
                    selection['notrecognised'] = thing

    # Delete all but the most specific of program_id, observation_id, data_label
    if 'data_label' in selection:
        selection.pop('observation_id', None)
        selection.pop('program_id', None)
    if 'observation_id' in selection:
        selection.pop('program_id', None)
    return selection


sayselection_defs = {
    'program_id': 'Program ID',
    'observation_id': 'Observation ID',
    'data_label': 'Data Label',
    'date': 'Date',
    'daterange': 'Daterange',
    'inst': 'Instrument',
    'observation_type': 'ObsType',
    'observation_class': 'ObsClass',
    'filename': 'Filename',
    'processing': 'Processing',
    'object': 'Object Name',
    'engineering': 'Engineering Data',
    'science_verification': 'Science Verification Data',
    'calprog': 'Calibration Program',
    'disperser': 'Disperser',
    'focal_plane_mask': 'Focal Plane Mask',
    'pupil_mask': 'Pupil Mask',
    'binning': 'Binning',
    'caltype': 'Calibration Type',
    'caloption': 'Calibration Option',
    'photstandard': 'Photometric Standard',
    'reduction': 'Reduction State',
    'twilight': 'Twilight',
    'az': 'Azimuth',
    'el': 'Elevation',
    'ra': 'RA',
    'dec': 'Dec',
    'sr': 'Search Radius',
    'crpa': 'CRPA',
    'telescope': 'Telescope',
    'detector_roi': 'Detector ROI',
    'detector_gain_setting': 'Gain',
    'detector_readspeed_setting': 'Read Speed',
    'detector_welldepth_setting': 'Well Depth',
    'detector_readmode_setting': 'Read Mode',
    'filepre': 'File Prefix',
    'mode': 'Spectroscopy Mode',
    'cenwlen': 'Central Wavelength',
    'camera': 'Camera',
    'exposure_time': 'Exposure Time',
    'coadds': 'Coadds',
    'mdready': 'MetaData OK',
    'gpi_astrometric_standard': 'GPI Astrometric Standard',
    'night': 'Observing Night',
    'nightrange': 'Observing Night Range',
    }


def sayselection(selection):
    """
    Returns a string that describes the selection dictionary passed in suitable
    for pasting into html.

    """
    # First we're going to try to collect various parts of the selection in
    # a list that we can join later.

    # Collect simple associations of the 'key: value' type from the
    # sayselection_defs dictionary
    parts = ["%s: %s" % (sayselection_defs[key], selection[key])
             for key in sayselection_defs if key in selection]
    
    if selection.get('site_monitoring'):
        parts.append('Is Site Monitoring Data')

    # More complicated selections from here on
    if 'spectroscopy' in selection:
        parts.append('Spectroscopy' if selection['spectroscopy'] else 'Imaging')

    if 'qa_state' in selection:
        qa_state_dict = {
            'Win': "Win (Pass or Usable)",
            'NotFail': "Not Fail",
            'Lucky': "Lucky (Pass or Undefined)"
            }

        sel = selection['qa_state']
        parts.append('QA State: ' + qa_state_dict.get(sel, sel))

    if 'ao' in selection:
        parts.append("Adaptive Optics in beam"
                     if selection['ao'] == 'AO'
                     else "No Adaptive Optics in beam")

    if 'lgs' in selection:
        parts.append("LGS" if selection['lgs'] == 'LGS' else "NGS")

    # If any of the previous tests contributed parts to the list, this will
    # create a return string like '; ...; ...; ...'. Otherwise we get an
    # empty string.
    ret = '; '.join([''] + parts)

    if 'notrecognised' in selection:
        return ret + ". WARNING: I didn't understand these (case-sensitive) " \
                     "words: %s" % selection['notrecognised']

    return ret


queryselection_filters = (
    ('present',               DiskFile.present),
    ('canonical',             DiskFile.canonical),
    ('science_verification',  Header.science_verification),
    ('program_id',            Header.program_id),
    ('observation_id',        Header.observation_id),
    ('data_label',            Header.data_label),
    ('observation_type',      Header.observation_type),
    ('observation_class',     Header.observation_class),
    ('reduction',             Header.reduction),
    ('telescope',             Header.telescope),
    ('filename',              File.name),
    ('binning',               Header.detector_binning),
    ('gain',                  Header.detector_gain_setting),
    ('readspeed',             Header.detector_readspeed_setting),
    ('welldepth',             Header.detector_welldepth_setting),
    ('readmode',              Header.detector_readmode_setting),
    ('filter',                Header.filter_name),
    ('spectroscopy',          Header.spectroscopy),
    ('mode',                  Header.mode),
    ('coadds',                Header.coadds),
    ('mdready',               DiskFile.mdready),
    ('site_monitoring',       Header.site_monitoring),
    ('calprog',               Header.calibration_program),
    ('pre_image',             Header.pre_image),
    ('raw_cc',                Header.raw_cc),
    ('raw_iq',                Header.raw_iq),
    ('processing',            Header.processing)
    )


# This function is used to add the stuff to stop it finding data by coords
# when the coords are proprietary.
def querypropcoords(query):
    return query.filter(or_(Header.proprietary_coordinates == False,
                            Header.release <= func.now()))


def queryselection(query, selection):
    """
    Given an sqlalchemy query object and a selection dictionary,
    add filters to the query for the items in the selection
    and return the query object
    """
    for key, field in queryselection_filters:
        if key in selection:
            query = query.filter(field == selection[key])

    # For some bizarre reason, doing a .in_([]) with an empty list is really
    # slow, and postgres eats CPU for a while doing it.
    if 'filelist' in selection:
        if selection['filelist']:
            query = query.filter(File.name.in_(selection['filelist']))
        else:
            query = query.filter(False)

    # Ignore the "Include" dummy value
    if selection.get('engineering') in (True, False):
        query = query.filter(Header.engineering == selection['engineering'])

    if selection.get('calprog') in (True, False):
        query = query.filter(Header.calibration_program == selection['calprog'])

    if ('object' in selection) and (
            ('ra' not in selection) and ('dec' not in selection)):
        # Handle the "wildcards" allowed on the object name
        object = selection['object']
        if object.startswith('*') or object.endswith('*'):
            # Wildcards are used, replace with SQL wildcards and use ilike query
            object = object.replace('*', '%')
        # ilike is a case-insensitive version of like
        query = query.filter(Header.object.ilike(object))
        query = querypropcoords(query)

    # Should we query by date?
    if 'date' in selection:
        # This is now a literal UTC date query. To query by observing night
        # use the 'night' selection

        startdt, enddt = gmu.get_time_period(selection['date'])

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Should we query by daterange?
    if 'daterange' in selection:
        # Parse the date to start and end datetime objects
        startd, endd = gmu.gemini_daterange(selection['daterange'],
                                            as_dates=True)
        startdt, enddt = gmu.get_time_period(startd, endd)

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Query by Observing Night
    if 'night' in selection:
        startdt, enddt = gmu.get_time_period(selection['night'])
        query = query.filter(
            or_(
                and_(Header.telescope == 'Gemini-North',
                     Header.ut_datetime >= startdt,
                     Header.ut_datetime < enddt),
                and_(Header.telescope == 'Gemini-South',
                     Header.ut_datetime >= startdt + gmu.CHILE_OFFSET,
                     Header.ut_datetime < enddt + gmu.CHILE_OFFSET)
            )
        )

    # Query by nightrange
    if 'nightrange' in selection:
        startd, endd = gmu.gemini_daterange(selection['nightrange'],
                                            as_dates=True)
        startdt, enddt = gmu.get_time_period(startd, endd)
        query = query.filter(
            or_(
                and_(Header.telescope == 'Gemini-North',
                     Header.ut_datetime >= startdt,
                     Header.ut_datetime < enddt),
                and_(Header.telescope == 'Gemini-South',
                     Header.ut_datetime >= startdt + gmu.CHILE_OFFSET,
                     Header.ut_datetime < enddt + gmu.CHILE_OFFSET)
        )
    )

    if 'inst' in selection:
        if selection['inst'] == 'GMOS':
            query = query.filter(or_(Header.instrument == 'GMOS-N',
                                     Header.instrument == 'GMOS-S'))
        else:
            query = query.filter(Header.instrument == selection['inst'])

    if 'disperser' in selection:
        if 'inst' in selection and selection['inst'] == 'GNIRS':
            if selection['disperser'] == '10lXD':
                query = query.filter(or_(Header.disperser == '10_mm&SXD',
                                         Header.disperser == '10_mm&LXD'))
            elif selection['disperser'] == '32lXD':
                query = query.filter(or_(Header.disperser == '32_mm&SXD',
                                         Header.disperser == '32_mm&LXD'))
            elif selection['disperser'] == '111lXD':
                query = query.filter(or_(Header.disperser == '111_mm&SXD',
                                         Header.disperser == '111_mm&LXD'))
            else:
                query = query.filter(Header.disperser == selection['disperser'])
        else:
            like_arg = selection['disperser'] + '_%'
            query = query.filter(
                or_(Header.disperser == selection['disperser'],
                    Header.disperser.like(like_arg)))

    if 'camera' in selection:
        # Hack for GNIRS camera names
        # - find both the Red and Blue options for each case
        if selection['camera'] == 'GnirsLong':
            query = query.filter(or_(Header.camera == 'LongRed',
                                     Header.camera == 'LongBlue'))
        elif selection['camera'] == 'GnirsShort':
            query = query.filter(or_(Header.camera == 'ShortRed',
                                     Header.camera == 'ShortBlue'))
        else:
            query = query.filter(Header.camera == selection['camera'])

    if 'focal_plane_mask' in selection:
        if 'inst' in list(selection.keys()) and selection['inst'] == 'TReCS':
            # handle the quotes and options "+ stuff" in the TReCS mask names.
            # the selection should only contain the "1.23" bit
            query = query.filter(
                Header.focal_plane_mask.contains(selection['focal_plane_mask']))
        if 'inst' in list(selection.keys()) and selection['inst'][:4] == 'GMOS':
            # Make this startswith for convenience finding multiple gmos masks
            query = query.filter(Header.focal_plane_mask.startswith(
                selection['focal_plane_mask']))
        else:
            query = query.filter(Header.focal_plane_mask ==
                                 selection['focal_plane_mask'])

    if 'pupil_mask' in selection:
        query = query.filter(Header.pupil_mask == selection['pupil_mask'])

    if 'qa_state' in selection and selection['qa_state'] != 'AnyQA':
        if selection['qa_state'] == 'Win':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Usable'))
        elif selection['qa_state'] == 'NotFail':
            query = query.filter(Header.qa_state != 'Fail')
        elif selection['qa_state'] == 'Lucky':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Undefined'))
        elif selection['qa_state'] == 'UndefinedQA':
            query = query.filter(Header.qa_state == 'Undefined')
        else:
            query = query.filter(Header.qa_state == selection['qa_state'])

    if 'ao' in selection:
        isAO = (selection['ao'] == 'AO')
        query = query.filter(Header.adaptive_optics == isAO)

    if 'lgs' in selection:
        isLGS = (selection['lgs'] == 'LGS')
        query = query.filter(Header.laser_guide_star == isLGS)

    if 'detector_roi' in selection:
        if selection['detector_roi'] == 'Full Frame':
            query = query.filter(
                or_(Header.detector_roi_setting == 'Fixed',
                    Header.detector_roi_setting == 'Full Frame'))
        else:
            query = query.filter(Header.detector_roi_setting ==
                                 selection['detector_roi'])

    if 'photstandard' in selection:
        query = query.filter(Header.phot_standard == True)

    if 'twilight' in selection:
        if selection['twilight']:
            query = query.filter(Header.object == 'Twilight')
        else:
            query = query.filter(Header.object != 'Twilight')

    if 'az' in selection:
        a, b = _parse_range(selection['az'])
        if a is not None and b is not None:
            query = query.filter(Header.azimuth >= a).filter(Header.azimuth < b)
            query = querypropcoords(query)

    if 'el' in selection:
        a, b = _parse_range(selection['el'])
        if a is not None and b is not None:
            query = query.filter(Header.elevation >= a).\
                filter(Header.elevation < b)
            query = querypropcoords(query)

    # cosdec value is used in 'ra' code below to scale the search radius
    cosdec = None
    if 'dec' in selection:
        valid = True
        # might be a range or a single value
        match = re.match(r"(-?[\d:\.]+)-(-?[\d:\.]+)", selection['dec'])
        if match is None:
            # single value
            degs = gmu.dectodeg(selection['dec'])
            if degs is None:
                # Invalid value.
                selection['warning'] = 'Invalid Dec format. ' \
                                       'Ignoring your Dec constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(selection.keys()):
                    sr = gmu.srtodeg(selection['sr'])
                    if sr is None:
                        selection['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        selection['sr'] = '180'
                        sr = gmu.srtodeg(selection['sr'])
                else:
                    # No search radius specified. Default it for them
                    selection['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    selection['sr'] = '180'
                    sr = gmu.srtodeg(selection['sr'])
                lower = degs - sr
                upper = degs + sr

                # Also set cosdec value here for use in 'ra' code below
                cosdec = math.cos(math.radians(degs))

        else:
            # Got two values
            lower = gmu.dectodeg(match.group(1))
            upper = gmu.dectodeg(match.group(2))
            if (lower is None) or (upper is None):
                selection['warning'] = 'Invalid Dec range format. ' \
                                       'Ignoring your Dec constraint.'
                valid = False
            else:
                # Also set cosdec value here for use in 'ra' code below
                degs = 0.5*(lower + upper)
                cosdec = math.cos(math.radians(degs))

        if valid and (lower is not None) and (upper is not None):
            # Negative dec ranges are usually specified backwards, eg -20 - -30
            if upper < lower:
                query = query.filter(Header.dec >= upper)\
                    .filter(Header.dec < lower)
            else:
                query = query.filter(Header.dec >= lower)\
                    .filter(Header.dec < upper)
            query = querypropcoords(query)

    if 'ra' in selection:
        valid = True
        # might be a range or a single value
        value = selection['ra'].split('-')
        if len(value) == 1:
            # single value
            degs = gmu.ratodeg(value[0])
            if degs is None:
                # Invalid value.
                selection['warning'] = 'Invalid RA format. ' \
                                       'Ignoring your RA constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(selection.keys()):
                    sr = gmu.srtodeg(selection['sr'])
                    if sr is None:
                        selection['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        selection['sr'] = '180'
                        sr = gmu.srtodeg(selection['sr'])
                else:
                    # No search radius specified. Default it for them
                    selection['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    selection['sr'] = '180'
                    sr = gmu.srtodeg(selection['sr'])

                # Don't apply a factor 15 as that is done in the conversion
                # to degrees. But we do need to account for the factor cos(
                # dec) here. We use the cosdec value from above here,
                # or assume 1.0 if it is not set
                cosdec = 1.0 if cosdec is None else cosdec
                sr /= cosdec
                lower = degs - sr
                upper = degs + sr

        elif len(value) == 2:
            # Got two values
            lower = gmu.ratodeg(value[0])
            upper = gmu.ratodeg(value[1])
            if (lower is None) or (upper is None):
                selection['warning'] = 'Invalid RA range format. ' \
                                       'Ignoring your RA constraint.'
                valid = False

        else:
            # Invalid string format for RA
            selection['warning'] = 'Invalid RA format. ' \
                                   'Ignoring your RA constraint.'
            valid = False

        if valid and (lower is not None) and (upper is not None):
            if upper > lower:
                query = query.filter(Header.ra >= lower).\
                    filter(Header.ra < upper)
            else:
                query = query.filter(or_(Header.ra >= lower, Header.ra < upper))
            query = querypropcoords(query)

    if 'exposure_time' in selection:
        valid = True
        expt = None
        lower = None
        upper = None
        # might be a range or a single value
        selection['exposure_time'] = selection['exposure_time'].replace(' ', '')
        match = re.match(r"([\d\.]+)-([\d\.]+)", selection['exposure_time'])
        if match is None:
            # single value
            try:
                expt = float(selection['exposure_time'])
            except:
                pass
            if expt is None:
                # Invalid format
                selection['warning'] = "Invalid format for exposure time, " \
                                       "ignoring it."
                valid = False
            else:
                # Valid single value. Set range
                lower = expt - 0.5
                if lower < 0.0:
                    lower = 0.0
                upper = expt + 0.5
        else:
            # Got two values
            try:
                lower = float(match.group(1))
                upper = float(match.group(2))
            except (ValueError, TypeError):
                selection['warning'] = 'Invalid format for exposure time ' \
                                       'range. Ignoring it.'
                valid = False

        if valid:
            query = query.filter(Header.exposure_time >= lower)\
                .filter(Header.exposure_time <= upper)

    if 'crpa' in selection:
        a, b = _parse_range(selection['crpa'])
        if a is not None and b is not None:
            query = query.filter(Header.cass_rotator_pa >= a)\
                .filter(Header.cass_rotator_pa < b)
            query = querypropcoords(query)

    if 'filepre' in selection:
        likestr = '%s%%' % selection['filepre']
        query = query.filter(File.name.like(likestr))

    if 'cenwlen' in selection:
        valid = True
        # Might be a single value or a range
        value = selection['cenwlen'].split('-')
        if len(value) == 1:
            # single value
            try:
                value = float(value[0])
                lower = value - 0.1
                upper = value + 0.1
            except:
                selection['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        elif len(value) == 2:
            # Range
            try:
                lower = float(value[0])
                upper = float(value[1])
            except:
                selection['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        else:
            selection['warning'] = 'Central Wavelength value is invalid ' \
                                   'and has been ignored'
            valid = False

        if valid and not ((0.2 < lower < 30) and (0.2 < upper < 30)):
            selection['warning'] = 'Invalid Central wavelength value. Value ' \
                                   'should be in microns, >0.2 and <30.0'
            if lower > upper:
                lower, upper = upper, lower
            if lower < 0.2:
                lower = 0.2
            if upper > 30:
                upper = 30
            if lower > 30 or upper < 0.2:
                # only reject the terms outright if they are out of range
                selection['warning'] = 'Invalid Central wavelength value. ' \
                                       'Value should be in microns, >0.2 and ' \
                                       '<30.0 - Ignoring terms'
                valid = False

        if valid and (lower > upper):
            lower, upper = upper, lower

        if valid:
            query = query.filter(Header.central_wavelength > lower)\
                .filter(Header.central_wavelength < upper)

    if 'publication' in selection:
        query = query.join(Program, Header.program_id == Program.program_id)\
            .join(ProgramPublication, Program.id == ProgramPublication.program_id)\
            .join(Publication, Publication.id == ProgramPublication.publication_id)\
            .filter(Publication.bibcode == selection['publication'])

    if 'PIname' in selection or 'ProgramText' in selection:
        query = query.join(Program, Header.program_id == Program.program_id)
        if 'PIname' in selection:
            query = query.filter(
                func.to_tsvector(Program.pi_coi_names)
                .match(' & '.join(selection['PIname'].split()))
                )
        if 'ProgramText' in selection:
            query = query.filter(
                func.to_tsvector(Program.title)
                .match(' & '.join(selection['ProgramText'].split()))
                )

    if 'gpi_astrometric_standard' in selection:
        query = query.join(Gpi, Gpi.header_id == Header.id)
        query = query.filter(Gpi.astrometric_standard ==
                             selection['gpi_astrometric_standard'])

    if 'standard' in selection:
        query = query.filter(Header.types.ilike('%''STANDARD''%'))

    if 'entrytimedaterange' in selection:
        try:
            a, b = selection['entrytimedaterange'].split('--')
            startfiledt = dateutil.parser.isoparse(a)
            endfiledt = dateutil.parser.isoparse(b)

            query = query.filter(DiskFile.entrytime >= startfiledt).filter(
                DiskFile.entrytime < endfiledt)
        except Exception:
            # parse error on datetime
            pass
    return query


def openquery(selection):
    """
    Returns a boolean to say if the selection is limited to a reasonable number
    of results - ie does it contain a date, daterange, prog_id, obs_id etc.
    returns True if this selection will likely return a large number of results
    """

    things = {'date', 'daterange', 'night', 'nightrange','program_id',
              'observation_id', 'data_label', 'filename', 'filepre', 'filelist'}
    selection_keys = set(selection)  # Makes a set out of selection.keys()

    # Are the previous two sets disjoint?
    return len(things & selection_keys) == 0


range_cre = re.compile(r'(-?\d*\.?\d*)-(-?\d*\.?\d*)')


def _parse_range(string):
    """
    Expects a string in the form '12.345-67.89' as per the co-ordinate searches.
    Returns a list with the two values
    """

    match = range_cre.match(string)
    a, b = None, None
    if match and len(match.groups()) == 2:
        m1, m2 = match.group(1), match.group(2)

        # Check that we can convert them to floats, but don't actually do so
        try:
            aa, bb = float(m1), float(m2)
            a, b = m1, m2
        except (ValueError, TypeError):
            pass

    return a, b


def selection_to_URL(selection, with_columns=False):
    """
    Receives a selection dictionary, parses values and converts to URL string
    """
    fsc = get_config()
    urlstring = ''

    # We only want one of data_label, observation_id, program_id in the URL,
    # the most specific one should carry.
    if 'data_label' in selection:
        selection.pop('observation_id', None)
        selection.pop('program_id', None)
    if 'observation_id' in selection:
        selection.pop('program_id', None)

    for key in selection:
        if key in {'warning', 'Search', 'ObsLogsOnly'}:
            # Don't put the warning text or search buttons in the URL
            pass
        elif key == 'data_label':
            # See if it is a valid data_label
            dl = gmu.GeminiDataLabel(selection[key])
            if dl.valid:
                # Regular form, just stuff it in
                urlstring += '/%s' % selection[key]
            else:
                # It's a non-standard one
                urlstring += '/datalabel=%s' % selection[key]
        elif key == 'observation_id':
            # See if it is a valid observation id, or if we need to add obsid=
            go = gmu.GeminiObservation(selection[key])
            if go.valid:
                # Regular obs id, just stuff it in
                urlstring += '/%s' % selection[key]
            else:
                # It's a non-standard one
                urlstring += '/obsid=%s' % selection[key]
        elif key == 'program_id':
            # See if it is a valid program id, or if we need to add progid=
            gp = gmu.GeminiProgram(selection[key])
            if gp.valid:
                # Regular program id, just stuff it in
                urlstring += '/%s' % selection[key]
            else:
                # It's a non-standard one
                urlstring += '/progid=%s' % selection[key]
        elif key == 'object':
            # We need to double-escape this because the webserver/wsgi code (
            # outside our control) will de-escape it for us and we'll be left
            # with, for instance, /s that we can't differentiate from those
            # in the path.
            urlstring += '/object=%s' % urllib.parse.quote(
                selection[key]).replace('/', '%252F')
        elif key == 'publication':
            urlstring += '/publication=%s' % urllib.parse.quote(selection[key])
        elif key == 'spectroscopy':
            if selection[key] is True:
                urlstring += '/spectroscopy'
            else:
                urlstring += '/imaging'
        elif key in {'ra', 'dec', 'sr', 'filter', 'cenwlen', 'disperser',
                     'camera', 'exposure_time', 'coadds', 'pupil_mask',
                     'PIname', 'ProgramText', 'gain', 'readspeed', 'welldepth',
                     'readmode', 'date', 'daterange', 'night', 'nightrange'}:
            urlstring += '/%s=%s' % (key, selection[key])
        elif key == 'cols':
            if with_columns:
                urlstring += '/cols=%s' % selection['cols']
        elif key == 'present':
            if selection[key] is True:
                urlstring += '/present'
            else:
                urlstring += '/notpresent'
        elif key == 'canonical':
            if selection[key] is True:
                urlstring += '/canonical'
            else:
                urlstring += '/notcanonical'
        elif key == 'twilight':
            if selection[key] is True:
                urlstring += '/twilight'
            else:
                urlstring += '/nottwilight'
        elif key == 'engineering':
            if selection[key] is True:
                urlstring += '/engineering'
            elif selection[key] is False:
                urlstring += '/notengineering'
            else:
                urlstring += '/includeengineering'
        elif key == 'calprog':
            if selection[key] is True:
                urlstring += '/calprog'
            elif selection[key] is False:
                urlstring += '/notcalprog'
        elif key == 'science_verification':
            if selection[key] is True:
                urlstring += '/science_verification'
            else:
                urlstring += '/notscience_verification'
        elif key == 'detector_roi':
            if selection[key] == 'Full Frame':
                urlstring += '/fullframe'
            elif selection[key] == 'Central Spectrum':
                urlstring += '/centralspectrum'
            elif selection[key] == 'Central Stamp':
                urlstring += '/centralstamp'
            elif selection[key] == 'Central768':
                urlstring += '/central768'
            elif selection[key] == 'Central512':
                urlstring += '/central512'
            elif selection[key] == 'Central256':
                urlstring += '/central256'
            else:
                urlstring += '/%s' % selection[key]
        elif key == 'focal_plane_mask':
            # if selection[key] == gmos_focal_plane_mask(selection[key]):
            #     urlstring += '/' + str(selection[key])
            # else:
            urlstring += '/mask=' + str(selection[key])
        elif key == 'filepre':
            urlstring += '/filepre=%s' % selection[key]
        elif key == 'site_monitoring':
            if selection[key] is True:
                urlstring += '/site_monitoring'
            else:
                urlstring += '/not_site_monitoring'
        else:
            urlstring += '/%s' % selection[key]

    return urlstring
