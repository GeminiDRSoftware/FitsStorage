from . import Selection

import fits_storage.gemini_metadata_utils as gmu

from fits_storage.config import get_config
fsc = get_config()

# These are factory functions that generate Selection(dict) instances.

# The from_url_things() function converts a list of elements from the URL
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
    'filepre': 'filepre',
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
    'cenwlen': 'cenwlen',
    'exposure_time': 'exposure_time',
    'coadds': 'coadds',
    'publication': 'publication',
    'PIname': 'PIname',
    'ProgramText': 'ProgramText',
    'raw_cc': 'raw_cc',
    'raw_iq': 'raw_iq',
    'raw_bg': 'raw_bg',
    'raw_wv': 'raw_wv',
    'gain': 'gain',
    'readspeed': 'readspeed',
    'date': 'date',
    'daterange': 'daterange',
    'night': 'night',
    'nightrange': 'nightrange',
    'entrytimedaterange': 'entrytimedaterange',
    'lastmoddaterange': 'lastmoddaterange',
    'processing_tag': 'processing_tag',
    'readmode': 'readmode',
    'welldepth': 'welldepth',
    'path': 'path',
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
    'NOAO': 'ao',
    }

# Some elements set a certain selection entry to boolean value...
getselection_booleans = {
    'defaults': ('defaults', True),
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


def from_url_things(things):
    """
    This takes a list of things from the URL, and returns a Selection dict.
    We disregard all but the most specific of a project id, observation id
    or datalabel.

    If a raw date, eg /YYYYMMDD is specified in the URL, whether it is treated
    as a UTC date or a "night" date is configuration dependent.
    """
    selection = Selection()

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
                selection['program_id'] = \
                    gmu.GeminiProgram(thing).program_id
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
                # Handle the custom escaping of '/' characters here
                selection['object'] = value.replace('=slash=', '/')
            elif thing in {'LS', 'MOS', 'IFS'}:
                selection['mode'] = thing
                selection['spectroscopy'] = True
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

    # Unpack defaults, if appropriate
    selection.unpackdefaults()
    return selection
