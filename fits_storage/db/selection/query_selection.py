import re
import math

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


def filterquery(self, query):
    """
    Given an sqlalchemy query object, add filters for the items in the selection
    and return the query object
    """
    for key, field in queryselection_filters:
        if key in self._seldict:
            query = query.filter(field == self._seldict[key])

    # For some bizarre reason, doing a .in_([]) with an empty list is really
    # slow, and postgres eats CPU for a while doing it.
    if 'filelist' in self._seldict:
        if self._seldict['filelist']:
            query = query.filter(File.name.in_(self._seldict['filelist']))
        else:
            query = query.filter(False)

    # Ignore the "Include" dummy value
    if self._seldict.get('engineering') in (True, False):
        query = query.filter(Header.engineering == self._seldict['engineering'])

    if self._seldict.get('calprog') in (True, False):
        query = query.filter(Header.calibration_program == self._seldict['calprog'])

    if ('object' in self._seldict) and (
            ('ra' not in self._seldict) and ('dec' not in self._seldict)):
        # Handle the "wildcards" allowed on the object name
        object = self._seldict['object']
        if object.startswith('*') or object.endswith('*'):
            # Wildcards are used, replace with SQL wildcards and use ilike query
            object = object.replace('*', '%')
        # ilike is a case-insensitive version of like
        query = query.filter(Header.object.ilike(object))
        query = querypropcoords(query)

    # Should we query by date?
    if 'date' in self._seldict:
        # This is now a literal UTC date query. To query by observing night
        # use the 'night' selection

        startdt, enddt = gmu.get_time_period(self._seldict['date'])

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Should we query by daterange?
    if 'daterange' in self._seldict:
        # Parse the date to start and end datetime objects
        startd, endd = gmu.gemini_daterange(self._seldict['daterange'],
                                            as_dates=True)
        startdt, enddt = gmu.get_time_period(startd, endd)

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Query by Observing Night
    if 'night' in self._seldict:
        startdt, enddt = gmu.get_time_period(self._seldict['night'])
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
    if 'nightrange' in self._seldict:
        startd, endd = gmu.gemini_daterange(self._seldict['nightrange'],
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

    if 'inst' in self._seldict:
        if self._seldict['inst'] == 'GMOS':
            query = query.filter(or_(Header.instrument == 'GMOS-N',
                                     Header.instrument == 'GMOS-S'))
        else:
            query = query.filter(Header.instrument == self._seldict['inst'])

    if 'disperser' in self._seldict:
        if 'inst' in self._seldict and self._seldict['inst'] == 'GNIRS':
            if self._seldict['disperser'] == '10lXD':
                query = query.filter(or_(Header.disperser == '10_mm&SXD',
                                         Header.disperser == '10_mm&LXD'))
            elif self._seldict['disperser'] == '32lXD':
                query = query.filter(or_(Header.disperser == '32_mm&SXD',
                                         Header.disperser == '32_mm&LXD'))
            elif self._seldict['disperser'] == '111lXD':
                query = query.filter(or_(Header.disperser == '111_mm&SXD',
                                         Header.disperser == '111_mm&LXD'))
            else:
                query = query.filter(Header.disperser == self._seldict['disperser'])
        else:
            like_arg = self._seldict['disperser'] + '_%'
            query = query.filter(
                or_(Header.disperser == self._seldict['disperser'],
                    Header.disperser.like(like_arg)))

    if 'camera' in self._seldict:
        # Hack for GNIRS camera names
        # - find both the Red and Blue options for each case
        if self._seldict['camera'] == 'GnirsLong':
            query = query.filter(or_(Header.camera == 'LongRed',
                                     Header.camera == 'LongBlue'))
        elif self._seldict['camera'] == 'GnirsShort':
            query = query.filter(or_(Header.camera == 'ShortRed',
                                     Header.camera == 'ShortBlue'))
        else:
            query = query.filter(Header.camera == self._seldict['camera'])

    if 'focal_plane_mask' in self._seldict:
        if 'inst' in list(self._seldict.keys()) and self._seldict['inst'] == 'TReCS':
            # handle the quotes and options "+ stuff" in the TReCS mask names.
            # the selection should only contain the "1.23" bit
            query = query.filter(
                Header.focal_plane_mask.contains(self._seldict['focal_plane_mask']))
        if 'inst' in list(self._seldict.keys()) and self._seldict['inst'][:4] == 'GMOS':
            # Make this startswith for convenience finding multiple gmos masks
            query = query.filter(Header.focal_plane_mask.startswith(
                self._seldict['focal_plane_mask']))
        else:
            query = query.filter(Header.focal_plane_mask ==
                                 self._seldict['focal_plane_mask'])

    if 'pupil_mask' in self._seldict:
        query = query.filter(Header.pupil_mask == self._seldict['pupil_mask'])

    if 'qa_state' in self._seldict and self._seldict['qa_state'] != 'AnyQA':
        if self._seldict['qa_state'] == 'Win':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Usable'))
        elif self._seldict['qa_state'] == 'NotFail':
            query = query.filter(Header.qa_state != 'Fail')
        elif self._seldict['qa_state'] == 'Lucky':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Undefined'))
        elif self._seldict['qa_state'] == 'UndefinedQA':
            query = query.filter(Header.qa_state == 'Undefined')
        else:
            query = query.filter(Header.qa_state == self._seldict['qa_state'])

    if 'ao' in self._seldict:
        isAO = (self._seldict['ao'] == 'AO')
        query = query.filter(Header.adaptive_optics == isAO)

    if 'lgs' in self._seldict:
        isLGS = (self._seldict['lgs'] == 'LGS')
        query = query.filter(Header.laser_guide_star == isLGS)

    if 'detector_roi' in self._seldict:
        if self._seldict['detector_roi'] == 'Full Frame':
            query = query.filter(
                or_(Header.detector_roi_setting == 'Fixed',
                    Header.detector_roi_setting == 'Full Frame'))
        else:
            query = query.filter(Header.detector_roi_setting ==
                                 self._seldict['detector_roi'])

    if 'photstandard' in self._seldict:
        query = query.filter(Header.phot_standard == True)

    if 'twilight' in self._seldict:
        if self._seldict['twilight']:
            query = query.filter(Header.object == 'Twilight')
        else:
            query = query.filter(Header.object != 'Twilight')

    if 'az' in self._seldict:
        a, b = _parse_range(self._seldict['az'])
        if a is not None and b is not None:
            query = query.filter(Header.azimuth >= a).filter(Header.azimuth < b)
            query = querypropcoords(query)

    if 'el' in self._seldict:
        a, b = _parse_range(self._seldict['el'])
        if a is not None and b is not None:
            query = query.filter(Header.elevation >= a).\
                filter(Header.elevation < b)
            query = querypropcoords(query)

    # cosdec value is used in 'ra' code below to scale the search radius
    cosdec = None
    if 'dec' in self._seldict:
        valid = True
        # might be a range or a single value
        match = re.match(r"(-?[\d:\.]+)-(-?[\d:\.]+)", self._seldict['dec'])
        if match is None:
            # single value
            degs = gmu.dectodeg(self._seldict['dec'])
            if degs is None:
                # Invalid value.
                self._seldict['warning'] = 'Invalid Dec format. ' \
                                       'Ignoring your Dec constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(self._seldict.keys()):
                    sr = gmu.srtodeg(self._seldict['sr'])
                    if sr is None:
                        self._seldict['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        self._seldict['sr'] = '180'
                        sr = gmu.srtodeg(self._seldict['sr'])
                else:
                    # No search radius specified. Default it for them
                    self._seldict['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    self._seldict['sr'] = '180'
                    sr = gmu.srtodeg(self._seldict['sr'])
                lower = degs - sr
                upper = degs + sr

                # Also set cosdec value here for use in 'ra' code below
                cosdec = math.cos(math.radians(degs))

        else:
            # Got two values
            lower = gmu.dectodeg(match.group(1))
            upper = gmu.dectodeg(match.group(2))
            if (lower is None) or (upper is None):
                self._seldict['warning'] = 'Invalid Dec range format. ' \
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

    if 'ra' in self._seldict:
        valid = True
        # might be a range or a single value
        value = self._seldict['ra'].split('-')
        if len(value) == 1:
            # single value
            degs = gmu.ratodeg(value[0])
            if degs is None:
                # Invalid value.
                self._seldict['warning'] = 'Invalid RA format. ' \
                                       'Ignoring your RA constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(self._seldict.keys()):
                    sr = gmu.srtodeg(self._seldict['sr'])
                    if sr is None:
                        self._seldict['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        self._seldict['sr'] = '180'
                        sr = gmu.srtodeg(self._seldict['sr'])
                else:
                    # No search radius specified. Default it for them
                    self._seldict['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    self._seldict['sr'] = '180'
                    sr = gmu.srtodeg(self._seldict['sr'])

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
                self._seldict['warning'] = 'Invalid RA range format. ' \
                                       'Ignoring your RA constraint.'
                valid = False

        else:
            # Invalid string format for RA
            self._seldict['warning'] = 'Invalid RA format. ' \
                                   'Ignoring your RA constraint.'
            valid = False

        if valid and (lower is not None) and (upper is not None):
            if upper > lower:
                query = query.filter(Header.ra >= lower).\
                    filter(Header.ra < upper)
            else:
                query = query.filter(or_(Header.ra >= lower, Header.ra < upper))
            query = querypropcoords(query)

    if 'exposure_time' in self._seldict:
        valid = True
        expt = None
        lower = None
        upper = None
        # might be a range or a single value
        self._seldict['exposure_time'] = self._seldict['exposure_time'].replace(' ', '')
        match = re.match(r"([\d\.]+)-([\d\.]+)", self._seldict['exposure_time'])
        if match is None:
            # single value
            try:
                expt = float(self._seldict['exposure_time'])
            except:
                pass
            if expt is None:
                # Invalid format
                self._seldict['warning'] = "Invalid format for exposure time, " \
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
                self._seldict['warning'] = 'Invalid format for exposure time ' \
                                       'range. Ignoring it.'
                valid = False

        if valid:
            query = query.filter(Header.exposure_time >= lower)\
                .filter(Header.exposure_time <= upper)

    if 'crpa' in self._seldict:
        a, b = _parse_range(self._seldict['crpa'])
        if a is not None and b is not None:
            query = query.filter(Header.cass_rotator_pa >= a)\
                .filter(Header.cass_rotator_pa < b)
            query = querypropcoords(query)

    if 'filepre' in self._seldict:
        likestr = '%s%%' % self._seldict['filepre']
        query = query.filter(File.name.like(likestr))

    if 'cenwlen' in self._seldict:
        valid = True
        # Might be a single value or a range
        value = self._seldict['cenwlen'].split('-')
        if len(value) == 1:
            # single value
            try:
                value = float(value[0])
                lower = value - 0.1
                upper = value + 0.1
            except:
                self._seldict['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        elif len(value) == 2:
            # Range
            try:
                lower = float(value[0])
                upper = float(value[1])
            except:
                self._seldict['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        else:
            self._seldict['warning'] = 'Central Wavelength value is invalid ' \
                                   'and has been ignored'
            valid = False

        if valid and not ((0.2 < lower < 30) and (0.2 < upper < 30)):
            self._seldict['warning'] = 'Invalid Central wavelength value. Value ' \
                                   'should be in microns, >0.2 and <30.0'
            if lower > upper:
                lower, upper = upper, lower
            if lower < 0.2:
                lower = 0.2
            if upper > 30:
                upper = 30
            if lower > 30 or upper < 0.2:
                # only reject the terms outright if they are out of range
                self._seldict['warning'] = 'Invalid Central wavelength value. ' \
                                       'Value should be in microns, >0.2 and ' \
                                       '<30.0 - Ignoring terms'
                valid = False

        if valid and (lower > upper):
            lower, upper = upper, lower

        if valid:
            query = query.filter(Header.central_wavelength > lower)\
                .filter(Header.central_wavelength < upper)

    if 'publication' in self._seldict:
        query = query.join(Program, Header.program_id == Program.program_id)\
            .join(ProgramPublication, Program.id == ProgramPublication.program_id)\
            .join(Publication, Publication.id == ProgramPublication.publication_id)\
            .filter(Publication.bibcode == self._seldict['publication'])

    if 'PIname' in self._seldict or 'ProgramText' in self._seldict:
        query = query.join(Program, Header.program_id == Program.program_id)
        if 'PIname' in self._seldict:
            query = query.filter(
                func.to_tsvector(Program.pi_coi_names)
                .match(' & '.join(self._seldict['PIname'].split()))
                )
        if 'ProgramText' in self._seldict:
            query = query.filter(
                func.to_tsvector(Program.title)
                .match(' & '.join(self._seldict['ProgramText'].split()))
                )

    if 'gpi_astrometric_standard' in self._seldict:
        query = query.join(Gpi, Gpi.header_id == Header.id)
        query = query.filter(Gpi.astrometric_standard ==
                             self._seldict['gpi_astrometric_standard'])

    if 'standard' in self._seldict:
        query = query.filter(Header.types.ilike('%''STANDARD''%'))

    return query


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
