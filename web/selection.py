"""
This module deals with the 'selection' concept.
Functions in this module are only used within FitsStorageWebSummary.
"""
from sqlalchemy import or_

from gemini_metadata_utils import gemini_telescope, gemini_instrument, gemini_date, gemini_daterange, gemini_observation_type, gemini_observation_class, gemini_reduction_state, gemini_caltype, gmos_gratingname, gmos_focal_plane_mask, gemini_fitsfilename, gemini_binning, GeminiDataLabel, GeminiObservation, GeminiProject, ratodeg, dectodeg, srtodeg

import dateutil.parser
import datetime
import re
import urllib

from orm.header import Header
from orm.diskfile import DiskFile
from orm.file import File
from orm.footprint import Footprint
from orm.photstandard import PhotStandardObs

from fits_storage_config import use_as_archive

def getselection(things):
    """
    this takes a list of things from the URL, and returns a
    selection hash that is used by the html generators
    """
    selection = {}
    while(len(things)):
        thing = things.pop(0)
        recognised = False
        if(gemini_telescope(thing)):
            selection['telescope'] = gemini_telescope(thing)
            recognised = True
        if(gemini_date(thing)):
            selection['date'] = gemini_date(thing)
            recognised = True
        if(gemini_daterange(thing)):
            selection['daterange'] = gemini_daterange(thing)
            recognised = True
        gp = GeminiProject(thing)
        if(gp.program_id):
            selection['program_id'] = thing
            recognised = True
        if(thing[:7] == 'progid='):
            selection['program_id'] = thing[7:]
            recognised = True
        go = GeminiObservation(thing)
        if(go.observation_id):
            selection['observation_id'] = thing
            recognised = True
        if(thing[:6] == 'obsid='):
            selection['observation_id'] = thing[6:]
            recognised = True
        gdl = GeminiDataLabel(thing)
        if(gdl.datalabel):
            selection['data_label'] = thing
            recognised = True
        if(gemini_instrument(thing, gmos=True)):
            selection['inst'] = gemini_instrument(thing, gmos=True)
            recognised = True
        if(gemini_fitsfilename(thing)):
            selection['filename'] = gemini_fitsfilename(thing)
            recognised = True
        if(thing[:9] == 'filename='):
            selection['filename'] = thing[9:]
            recognised = True
        if(gemini_observation_type(thing)):
            selection['observation_type'] = gemini_observation_type(thing)
            recognised = True
        if(gemini_observation_class(thing)):
            selection['observation_class'] = gemini_observation_class(thing)
            recognised = True
        if(gemini_caltype(thing)):
            selection['caltype'] = gemini_caltype(thing)
            recognised = True
        if(gemini_reduction_state(thing)):
            selection['reduction'] = gemini_reduction_state(thing)
            recognised = True
        if(gmos_gratingname(thing)):
            selection['gmos_grating'] = gmos_gratingname(thing)
            recognised = True
        if(gmos_focal_plane_mask(thing)):
            selection['gmos_focal_plane_mask'] = gmos_focal_plane_mask(thing)
            recognised = True
        if(thing[:5] == 'mask='):
            selection['gmos_focal_plane_mask'] = thing[5:]
            recognised = True
        if(thing == 'warnings' or thing == 'missing' or thing == 'requires' or thing == 'takenow'):
            selection['caloption'] = thing
            recognised = True
        if(thing == 'imaging'):
            selection['spectroscopy'] = False
            recognised = True
        if(thing == 'spectroscopy'):
            selection['spectroscopy'] = True
            recognised = True
        if(thing in ['Pass', 'Usable', 'Fail', 'Win', 'NotFail', 'Lucky', 'AnyQA']):
            selection['qa_state'] = thing
            recognised = True
        if(thing == 'LGS' or thing == 'NGS'):
            selection['lgs'] = thing
            # Make LGS / NGS selection imply AO selection
            selection['ao'] = 'AO'
            recognised = True
        if(thing == 'AO' or thing == 'NOTAO'):
            selection['ao'] = thing
            recognised = True
        if(thing == 'present' or thing == 'Present'):
            selection['present'] = True
            recognised = True
        if(thing == 'notpresent' or thing == 'NotPresent'):
            selection['present'] = False
            recognised = True
        if(thing == 'canonical' or thing == 'Canonical'):
            selection['canonical'] = True
            recognised = True
        if(thing == 'notcanonical' or thing == 'NotCanonical'):
            selection['canonical'] = False
            recognised = True
        if(thing == 'engineering'):
            selection['engineering'] = True
            recognised = True
        if(thing == 'notengineering'):
            selection['engineering'] = False
            recognised = True
        if(thing == 'includeengineering'):
            # this is basically a dummy value for the search form defaults
            selection['engineering'] = 'Include'
            recognised = True
        if(thing == 'science_verification'):
            selection['science_verification'] = True
            recognised = True
        if(thing == 'notscience_verification'):
            selection['science_verification'] = False
            recognised = True
        if(thing[:7] == 'filter=' or thing[:7] == 'Filter='):
            selection['filter'] = thing[7:]
            recognised = True
        if(thing[:7] == 'object=' or thing[:7] == 'Object='):
            selection['object'] = urllib.unquote_plus(thing[7:])
            recognised = True
        if(gemini_binning(thing)):
            selection['binning'] = gemini_binning(thing)
            recognised = True
        if(thing == 'photstandard'):
            selection['photstandard'] = True
            recognised = True
        if(thing in ['low', 'high', 'slow', 'fast', 'NodAndShuffle', 'Classic']):
            if(not selection.has_key('detector_config')):
                selection['detector_config'] = []
            selection['detector_config'].append(thing)
            recognised = True
        if(thing.lower() in ['fullframe', 'centralstamp', 'centralspectrum']):
            if(thing.lower() == 'fullframe'):
                selection['detector_roi'] = 'Full Frame'
            if(thing.lower() == 'centralstamp'):
                selection['detector_roi'] = 'Central Stamp'
            if(thing.lower() == 'centralspectrum'):
                selection['detector_roi'] = 'Central Spectrum'
            recognised = True
        if(thing.lower() == 'twilight'):
            selection['twilight'] = True
            recognised = True
        if(thing.lower() == 'nottwilight'):
            selection['twilight'] = False
            recognised = True
        if(thing[:3] == 'az=' or thing[:3] == 'Az='):
            selection['az'] = thing[3:]
            recognised = True
        if(thing[:8] == 'azimuth=' or thing[:8] == 'Azimuth='):
            selection['az'] = thing[8:]
            recognised = True
        if(thing[:3] == 'el=' or thing[:3] == 'El='):
            selection['el'] = thing[3:]
            recognised = True
        if(thing[:10] == 'elevation=' or thing[:10] == 'Elevation='):
            selection['el'] = thing[10:]
            recognised = True
        if(thing[:3] == 'ra=' or thing[:3] == 'RA='):
            selection['ra'] = thing[3:]
            recognised = True
        if(thing[:4] == 'dec=' or thing[:4] == 'Dec='):
            selection['dec'] = thing[4:]
            recognised = True
        if(thing[:3] == 'sr=' or thing[:3] == 'SR='):
            selection['sr'] = thing[3:]
            recognised = True
        if(thing[:5] == 'crpa=' or thing[:5] == 'CRPA='):
            selection['crpa'] = thing[5:]
            recognised = True
        if((thing[:4] in ['N200', 'N201', 'N202', 'S200', 'S201', 'S202']) and (len(thing)<14)):
            # Good through 2029, don't match full filenames :-)
            selection['filepre'] = thing
            recognised = True
        if(thing[:8] == 'filepre='):
            selection['filepre'] = thing[8:]
            recognised = True
        if(thing in ['LS', 'MOS', 'IFU']):
            selection['mode'] = thing
            selection['spectroscopy'] = True
            recognised = True
        if(thing[:8] == 'cenwlen='):
            selection['cenwlen'] = thing[8:]
            recognised = True

        if(not recognised):
            if('notrecognised' in selection):
                selection['notrecognised'] += " "+thing
            else:
                selection['notrecognised'] = thing
    return selection

def sayselection(selection):
    """
    returns a string that describes the selection dictionary passed in
    suitable for pasting into html
    """
    string = ""

    defs = {'program_id': 'Program ID', 'observation_id': 'Observation ID', 'data_label': 'Data Label', 'date': 'Date', 'daterange': 'Daterange', 'inst':'Instrument', 'observation_type':'ObsType', 'observation_class': 'ObsClass', 'filename': 'Filename', 'object': 'Object Name', 'engineering': 'Engineering Data', 'science_verification': 'Science Verification Data', 'gmos_grating': 'GMOS Grating', 'gmos_focal_plane_mask': 'GMOS FP Mask', 'binning': 'Binning', 'caltype': 'Calibration Type', 'caloption': 'Calibration Option', 'photstandard': 'Photometric Standard', 'reduction': 'Reduction State', 'twilight': 'Twilight', 'az': 'Azimuth', 'el': 'Elevation', 'ra': 'RA', 'dec': 'Dec', 'sr': 'Search Radius', 'crpa': 'CRPA', 'telescope': 'Telescope', 'detector_roi': 'Detector ROI', 'filepre': 'File Prefix', 'mode': 'Spectroscopy Mode', 'cenwlen': 'Central Wavelength'}
    for key in defs:
        if key in selection:
            string += "; %s: %s" % (defs[key], selection[key])

    if('spectroscopy' in selection):
        if(selection['spectroscopy']):
            string += "; Spectroscopy"
        else:
            string += "; Imaging"
    if('qa_state' in selection):
        if(selection['qa_state'] == 'Win'):
            string += "; QA State: Win (Pass or Usable)"
        elif(selection['qa_state'] == 'NotFail'):
            string += "; QA State: Not Fail"
        elif(selection['qa_state'] == 'Lucky'):
            string += "; QA State: Lucky (Pass or Undefined)"
        else:
            string += "; QA State: %s" % selection['qa_state']
    if('ao' in selection):
        if(selection['ao'] == 'AO'):
            string += "; Adaptive Optics in beam"
        else:
            string += "; No Adaptive Optics in beam"
    if('lgs' in selection):
        if(selection['lgs'] == 'LGS'):
            string += "; LGS"
        else:
            string += "; NGS"
    if('detector_config' in selection):
        string += "; Detector Config: " + '+'.join(selection['detector_config'])

    if('notrecognised' in selection):
        string += ". WARNING: I didn't understand these (case-sensitive) words: %s" % selection['notrecognised']

    return string

# import time module to get local timezone
import time
def queryselection(query, selection):
    """
    Given an sqlalchemy query object and a selection dictionary,
    add filters to the query for the items in the selection
    and return the query object
    """

    # Do want to select Header object for which diskfile.present is true?
    if('present' in selection):
        query = query.filter(DiskFile.present == selection['present'])

    if('canonical' in selection):
        query = query.filter(DiskFile.canonical == selection['canonical'])

    if('engineering' in selection):
        # Ignore the "Inlcude" dummy value
        if(selection['engineering'] in [True, False]):
            query = query.filter(Header.engineering == selection['engineering'])

    if('science_verification' in selection):
        query = query.filter(Header.science_verification == selection['science_verification'])

    if('program_id' in selection):
        query = query.filter(Header.program_id == selection['program_id'])

    if('observation_id' in selection):
        query = query.filter(Header.observation_id == selection['observation_id'])

    if('data_label' in selection):
        query = query.filter(Header.data_label == selection['data_label'])

    if('object' in selection):
        query = query.filter(Header.object == selection['object'])

    # Should we query by date?
    if('date' in selection):
        # If this is an archive server, take the date very literally.
        # For the local fits servers, we do some manipulation to treat
        # it as an observing night...
        oneday = datetime.timedelta(days=1)

        if(use_as_archive):
            startdt = dateutil.parser.parse("%s 00:00:00" % (selection['date']))
            enddt = startdt + oneday
        else:
            # Parse the date to start and end datetime objects
            # We consider the night boundary to be 14:00 local time
            # This is midnight UTC in Hawaii, completely arbitrary in Chile
            startdt = dateutil.parser.parse("%s 14:00:00" % (selection['date']))
            if(time.daylight):
                tzoffset = datetime.timedelta(seconds=time.altzone)
            else:
                tzoffset = datetime.timedelta(seconds=time.timezone)
            startdt = startdt + tzoffset - oneday
            enddt = startdt + oneday

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime < enddt)

    # Should we query by daterange?
    if('daterange' in selection):
        # Parse the date to start and end datetime objects
        daterangecre = re.compile('([12][90]\d\d[01]\d[0123]\d)-([12][90]\d\d[01]\d[0123]\d)')
        m = daterangecre.match(selection['daterange'])
        startdate = m.group(1)
        enddate = m.group(2)
        tzoffset = datetime.timedelta(seconds=time.timezone)
        oneday = datetime.timedelta(days=1)
        # same as for date regarding archive server
        if(use_as_archive):
            startdt = dateutil.parser.parse("%s 00:00:00" % startdate)
            enddt = dateutil.parser.parse("%s 00:00:00" % enddate)
            enddt = enddt + oneday
        else:
            startdt = dateutil.parser.parse("%s 14:00:00" % startdate)
            startdt = startdt + tzoffset - oneday
            enddt = dateutil.parser.parse("%s 14:00:00" % enddate)
            enddt = enddt + tzoffset - oneday
            enddt = enddt + oneday
        # Flip them round if reversed
        if(startdt > enddt):
            tmp = enddt
            enddt = startdt
            startdt = tmp
        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime <= enddt)

    if('observation_type' in selection):
        query = query.filter(Header.observation_type == selection['observation_type'])

    if('observation_class' in selection):
        query = query.filter(Header.observation_class == selection['observation_class'])

    if('reduction' in selection):
        query = query.filter(Header.reduction == selection['reduction'])

    if('telescope' in selection):
        query = query.filter(Header.telescope == selection['telescope'])

    if('inst' in selection):
        if(selection['inst'] == 'GMOS'):
            query = query.filter(or_(Header.instrument == 'GMOS-N', Header.instrument == 'GMOS-S'))
        else:
            query = query.filter(Header.instrument == selection['inst'])

    if('filename' in selection):
        query = query.filter(File.name == selection['filename'])

    if('filelist' in selection):
        query = query.filter(File.name.in_(selection['filelist']))

    if('gmos_grating' in selection):
        query = query.filter(Header.disperser == selection['gmos_grating'])

    if('gmos_focal_plane_mask' in selection):
        query = query.filter(Header.focal_plane_mask == selection['gmos_focal_plane_mask'])

    if('spectroscopy' in selection):
        query = query.filter(Header.spectroscopy == selection['spectroscopy'])

    if('mode' in selection):
        query = query.filter(Header.mode == selection['mode'])

    if('qa_state' in selection):
        if(selection['qa_state'] == 'Win'):
            query = query.filter(or_(Header.qa_state=='Pass', Header.qa_state=='Usable'))
        elif(selection['qa_state'] == 'NotFail'):
            query = query.filter(Header.qa_state!='Fail')
        elif(selection['qa_state'] == 'Lucky'):
            query = query.filter(or_(Header.qa_state=='Pass', Header.qa_state=='Undefined'))
        elif(selection['qa_state'] == 'AnyQA'):
            # Dummy value to enable persistence in the search form
            pass
        else:
            query = query.filter(Header.qa_state == selection['qa_state'])

    if('ao' in selection):
        if(selection['ao'] == 'AO'):
            query = query.filter(Header.adaptive_optics == True)
        else:
            query = query.filter(Header.adaptive_optics == False)

    if('lgs' in selection):
        if(selection['lgs'] == 'LGS'):
            query = query.filter(Header.laser_guide_star == True)
        else:
            query = query.filter(Header.laser_guide_star == False)

    if('binning' in selection):
        query = query.filter(Header.detector_binning == selection['binning'])

    if('detector_roi' in selection):
        if(selection['detector_roi'] == 'Full Frame'):
            query = query.filter(or_(Header.detector_roi_setting == 'Fixed', Header.detector_roi_setting == 'Full Frame'))
        else:
            query = query.filter(Header.detector_roi_setting == selection['detector_roi'])

    if('filter' in selection):
        query = query.filter(Header.filter_name == selection['filter'])

    if('photstandard' in selection):
        query = query.filter(Footprint.header_id == Header.id)
        query = query.filter(PhotStandardObs.footprint_id == Footprint.id)

    if('detector_config' in selection):
        for thing in selection['detector_config']:
            if thing == 'Classic':
                query = query.filter(~Header.detector_config.contains('NodAndShuffle'))
            else:
                query = query.filter(Header.detector_config.contains(thing))

    if('twilight' in selection):
        if(selection['twilight'] == True):
            query = query.filter(Header.object == 'Twilight')
        if(selection['twilight'] == False):
            query = query.filter(Header.object != 'Twilight')

    if('az' in selection):
        [a, b] = _parse_range(selection['az'])
        if(a is not None and b is not None):
            query = query.filter(Header.azimuth >= a).filter(Header.azimuth < b)

    if('el' in selection):
        [a, b] = _parse_range(selection['el'])
        if(a is not None and b is not None):
            query = query.filter(Header.elevation >= a).filter(Header.elevation < b)

    if('ra' in selection):
        valid = True
        # might be a range or a single value
        value = selection['ra'].split('-')
        if (len(value) == 1):
            # single value
            degs = ratodeg(value[0])
            if (degs is None):
                # Invalid value.
                selection['warning'] = 'Invalid RA format. Ignoring your RA constraint.'
                valid = False
            # valid single value, get search radius
            if 'sr' in selection.keys():
                sr = srtodeg(selection['sr'])
                if (sr is None):
                    selection['warning'] = 'Invalid Search Radius, defaulting to 3 arcmin'
                    selection['sr'] = 180
                    sr = srtodeg(selection['sr'])
            else:
                # No search radius specified. Default it for them
                selection['warning'] = 'No Search Radius given, defaulting to 3 arcmin'
                selection['sr'] = 180
                sr = srtodeg(selection['sr'])
            if (valid):
                lower = degs - sr
                upper = degs + sr

        elif (len(value) == 2):
            # Got two values
            lower = ratodeg(value[0])
            upper = ratodeg(value[1])
            if((lower is None) or (upper is None)):
                selection['warning'] = 'Invalid RA range format. Ignoring your RA constraint.'
                valid = False

        else:
            # Invalid string format for RA
            selection['warning'] = 'Invalid RA format. Ignoring your RA constraint.'
            valid = False

        if valid and (lower is not None) and (upper is not None):
            if upper > lower:
                query = query.filter(Header.ra >= lower).filter(Header.ra < upper)
            else: 
                query = query.filter(or_(Header.ra >= lower, Header.ra < upper))

    if('dec' in selection):
        valid = True
        # might be a range or a single value
        match = re.match("(-?[\d:\.]+)-(-?[\d:\.]+)", selection['dec'])
        if(match is None):
            # single value
            degs = dectodeg(selection['dec'])
            if (degs is None):
                # Invalid value.
                selection['warning'] = 'Invalid Dec format. Ignoring your Dec constraint.'
                valid = False
            # valid single value, get search radius
            if 'sr' in selection.keys():
                sr = srtodeg(selection['sr'])
                if (sr is None):
                    selection['warning'] = 'Invalid Search Radius, defaulting to 3 arcmin'
                    selection['sr'] = 180
                    sr = srtodeg(selection['sr'])
            else:
                # No search radius specified. Default it for them
                selection['warning'] = 'No Search Radius given, defaulting to 3 arcmin'
                selection['sr'] = 180
                sr = srtodeg(selection['sr'])
            if (valid):
                lower = degs - sr
                upper = degs + sr

        else:
            # Got two values
            lower = dectodeg(match.group(1))
            upper = dectodeg(match.group(2))
            if((lower is None) or (upper is None)):
                selection['warning'] = 'Invalid Dec range format. Ignoring your Dec constraint.'
                valid = False

        if valid and (lower is not None) and (upper is not None):
            query = query.filter(Header.dec >= lower).filter(Header.dec < upper)


    if('crpa' in selection):
        [a, b] = _parse_range(selection['crpa'])
        if(a is not None and b is not None):
            query = query.filter(Header.cass_rotator_pa >= a).filter(Header.cass_rotator_pa < b)

    if('filepre' in selection):
        likestr = '%s%%' % selection['filepre']
        query = query.filter(File.name.like(likestr))

    if('cenwlen' in selection):
        valid = True
        # Might be a single value or a range
        value = selection['cenwlen'].split('-')
        if(len(value) == 1):
            # single value
            try:
                value = float(value[0])
                lower = value - 0.1
                upper = value + 0.1
            except:
                selection['warning'] = 'Central Wavelength value is invalid and has been ignored'
                valid = False
        elif (len(value) == 2):
            # Range
            try:
                lower = float(value[0])
                upper = float(value[1])
            except:
                selection['warning'] = 'Central Wavelength value is invalid and has been ignored'
                valid = False
        else:
            selection['warning'] = 'Central Wavelength value is invalid and has been ignored'
            valid = False

        if valid and ((lower > 30.0) or (lower < 0.2) or (upper > 30.0) or (upper < 0.2)):
            selection['warning'] = 'Invalid Central wavelength value. Value should be in microns, >0.2 and <30.0'
            valid = False

        if valid and (lower > upper):
            # swap them over
            tmp = lower
            lower = upper
            upper = tmp

        if(valid):
            query = query.filter(Header.central_wavelength > lower).filter(Header.central_wavelength < upper)

    return query

def openquery(selection):
    """
    Returns a boolean to say if the selection is limited to a reasonable number of
    results - ie does it contain a date, daterange, prog_id, obs_id etc
    returns True if this selection will likely return a large number of results
    """
    openquery = True

    things = ['date', 'daterange', 'program_id', 'observation_id', 'data_label', 'filename', 'filepre']

    for thing in things:
        if(thing in selection):
            openquery = False

    return openquery

range_cre = re.compile('(-?\d*\.?\d*)-(-?\d*\.?\d*)')
def _parse_range(string):
    """
    Expects a string in the form '12.345-67.89' as per the co-ordinate searches.
    Returns a list with the two values
    """

    match = range_cre.match(string)
    a = None
    b = None
    if(match and len(match.groups()) == 2):
        a = match.group(1)
        b = match.group(2)

        # Check that we can convert them to floats, but don't actually do so
        try:
            aa = float(a)
            bb = float(b)
        except (ValueError, TypeError):
            a = None
            b = None

        return [a, b]

def selection_to_URL(selection):
    """
    Receives a selection dictionary, parses values and converts to URL string
    """
    urlstring = ''

    for key in selection.keys():
        if key == 'program_id':
            # See if it is a valid program id, or if we need to add progid=
            gp = GeminiProject(selection[key])
            if(gp.program_id):
                # Regular program id, just stuff it in
                urlstring += '/%s' % selection[key]
            else:
                # It's a non standard one
                urlstring += '/progid=%s' % selection[key]
        elif key == 'object':
            urlstring += '/object=%s' % urllib.quote_plus(selection[key])
        elif key == 'spectroscopy':
            if (selection[key] is True):
                urlstring += '/spectroscopy'
            else:
                urlstring += '/imaging'
        elif key in ['ra', 'dec', 'sr', 'filter', 'cenwlen']:
            urlstring += '/%s=%s' % (key, selection[key])
        elif key == 'present':
            if (selection[key] is True):
                urlstring += '/present'
            else:
                urlstring += '/notpresent'
        elif key == 'canonical':
            if (selection[key] is True):
                urlstring += '/canonical'
            else:
                urlstring += '/notcanonical'
        elif key == 'twilight':
            if (selection[key] is True):
                urlstring += '/twilight'
            else:
                urlstring += '/nottwilight'
        elif key == 'engineering':
            if (selection[key] is True):
                urlstring += '/engineering'
            elif (selection[key] is False):
                urlstring += '/notengineering'
            else:
                urlstring += '/includeengineering'

        elif key == 'science_verification':
            if (selection[key] is True):
                urlstring += '/science_verification'
            else:
                urlstring += '/notscience_verification'
        elif key == 'detector_roi':
            if (selection[key] == 'Full Frame'):
                urlstring += '/fullframe'
            if (selection[key] == 'Central Spectrum'):
                urlstring += '/centralspectrum'
            if (selection[key] == 'Central Stamp'):
                urlstring += '/centralstamp'
        elif key == 'gmos_focal_plane_mask':
            if(selection[key] == gmos_focal_plane_mask(selection[key])):
                urlstring += '/' + str(selection[key])
            else:
                urlstring += '/mask=' + str(selection[key])
        elif key == 'detector_config':
            for config in selection[key]:
                urlstring += '/%s' % config
        else:
            urlstring += '/%s' % selection[key]
    
    return urlstring
