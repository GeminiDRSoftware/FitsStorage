import re
import math

from sqlalchemy import or_, and_, func, case

import fits_storage.gemini_metadata_utils as gmu

# TODO - get rid of this link into the GPI table
from fits_storage.cal.orm.gpi import Gpi

from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File

from fits_storage.config import get_config
fsc = get_config()

if fsc.is_server:
    from fits_storage.server.orm.program import Program
    from fits_storage.server.orm.publication import (Publication,
                                                     ProgramPublication)
    # To support default processing tags:
    from fits_storage.db.list_headers import default_processing_tags

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
    ('processing',            Header.processing),
    )


# This function is used to add the stuff to stop it finding data by coords
# when the coords are proprietary.
def querypropcoords(query):
    return query.filter(or_(Header.proprietary_coordinates == False,
                            Header.release <= func.now()))


def filter(self, query, ignore_processing_tag=False):
    """
    Given an sqlalchemy query object, add filters for the items in the selection
    and return the query object
    """
    for key, field in queryselection_filters:
        if key in self:
            query = query.filter(field == self[key])

    # For some bizarre reason, doing a .in_([]) with an empty list is really
    # slow, and postgres eats CPU for a while doing it.
    # The format of the filelist entries is either "filename" if path == '' or "path.filename" otherwise.
    if 'filelist' in self:
        if self['filelist']:
            query = query.filter(
                case(
                    (DiskFile.path=='', DiskFile.filename),
                    else_=DiskFile.path.concat('/').concat(DiskFile.filename))
                .in_(self['filelist']))
        else:
            query = query.filter(False)

    # Ignore the "Include" dummy value
    if self.get('engineering') in (True, False):
        query = query.filter(Header.engineering == self['engineering'])

    if self.get('calprog') in (True, False):
        query = query.filter(Header.calibration_program == self['calprog'])

    if ('object' in self) and (
            ('ra' not in self) and ('dec' not in self)):
        # Handle the "wildcards" allowed on the object name
        object = self['object']
        if object.startswith('*') or object.endswith('*'):
            # Wildcards are used, replace with SQL wildcards and use ilike query
            object = object.replace('*', '%')
        # ilike is a case-insensitive version of like
        query = query.filter(Header.object.ilike(object))
        query = querypropcoords(query)

    # Should we query by date?
    if 'date' in self:
        # This is now a literal UTC date query. To query by observing night
        # use the 'night' selection

        startdt, enddt = gmu.get_time_period(self['date'])

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Should we query by daterange?
    if 'daterange' in self:
        # Parse the date to start and end datetime objects
        startd, endd = gmu.gemini_daterange(self['daterange'],
                                            as_dates=True)
        startdt, enddt = gmu.get_time_period(startd, endd)

        # check it's between these two
        query = query.filter(Header.ut_datetime >= startdt)\
            .filter(Header.ut_datetime < enddt)

    # Query by Observing Night
    if 'night' in self:
        startdt, enddt = gmu.get_time_period(self['night'])
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
    if 'nightrange' in self:
        startd, endd = gmu.gemini_daterange(self['nightrange'],
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

    if 'entrytimedaterange' in self:
        pair = gmu.gemini_daterange(self['entrytimedaterange'],
                                              as_dates=True)
        if pair is not None:
            start, end = pair

            query = query.filter(DiskFile.entrytime >= start)\
                .filter(DiskFile.entrytime < end)


    if 'lastmoddaterange' in self:
        pair = gmu.gemini_daterange(self['lastmoddaterange'],
                                    as_dates=True)
        if pair is not None:
            start, end = pair

            query = query.filter(DiskFile.lastmod >= start) \
                .filter(DiskFile.lastmod < end)

    if 'inst' in self:
        if self['inst'] == 'GMOS':
            query = query.filter(or_(Header.instrument == 'GMOS-N',
                                     Header.instrument == 'GMOS-S'))
        else:
            query = query.filter(Header.instrument == self['inst'])

    if 'disperser' in self:
        if 'inst' in self and self['inst'] == 'GNIRS':
            if self['disperser'] == '10lXD':
                query = query.filter(or_(Header.disperser == '10_mm&SXD',
                                         Header.disperser == '10_mm&LXD'))
            elif self['disperser'] == '32lXD':
                query = query.filter(or_(Header.disperser == '32_mm&SXD',
                                         Header.disperser == '32_mm&LXD'))
            elif self['disperser'] == '111lXD':
                query = query.filter(or_(Header.disperser == '111_mm&SXD',
                                         Header.disperser == '111_mm&LXD'))
            else:
                query = query.filter(Header.disperser == self['disperser'])
        else:
            like_arg = self['disperser'] + '_%'
            query = query.filter(
                or_(Header.disperser == self['disperser'],
                    Header.disperser.like(like_arg)))

    if 'camera' in self:
        # Hack for GNIRS camera names
        # - find both the Red and Blue options for each case
        if self['camera'] == 'GnirsLong':
            query = query.filter(or_(Header.camera == 'LongRed',
                                     Header.camera == 'LongBlue'))
        elif self['camera'] == 'GnirsShort':
            query = query.filter(or_(Header.camera == 'ShortRed',
                                     Header.camera == 'ShortBlue'))
        else:
            query = query.filter(Header.camera == self['camera'])

    if 'focal_plane_mask' in self:
        if 'inst' in list(self.keys()) and self['inst'] == 'TReCS':
            # handle the quotes and options "+ stuff" in the TReCS mask names.
            # the selection should only contain the "1.23" bit
            query = query.filter(
                Header.focal_plane_mask.contains(self['focal_plane_mask']))
        if 'inst' in list(self.keys()) and self['inst'][:4] == 'GMOS':
            # Make this startswith for convenience finding multiple gmos masks
            query = query.filter(Header.focal_plane_mask.startswith(
                self['focal_plane_mask']))
        else:
            query = query.filter(Header.focal_plane_mask ==
                                 self['focal_plane_mask'])

    if 'pupil_mask' in self:
        query = query.filter(Header.pupil_mask == self['pupil_mask'])

    if 'qa_state' in self and self['qa_state'] != 'AnyQA':
        if self['qa_state'] == 'Win':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Usable'))
        elif self['qa_state'] == 'NotFail':
            query = query.filter(Header.qa_state != 'Fail')
        elif self['qa_state'] == 'Lucky':
            query = query.filter(or_(Header.qa_state == 'Pass',
                                     Header.qa_state == 'Undefined'))
        elif self['qa_state'] == 'UndefinedQA':
            query = query.filter(Header.qa_state == 'Undefined')
        else:
            query = query.filter(Header.qa_state == self['qa_state'])

    if 'ao' in self:
        isAO = (self['ao'] == 'AO')
        query = query.filter(Header.adaptive_optics == isAO)

    if 'lgs' in self:
        isLGS = (self['lgs'] == 'LGS')
        query = query.filter(Header.laser_guide_star == isLGS)

    if 'detector_roi' in self:
        if self['detector_roi'] == 'Full Frame':
            query = query.filter(
                or_(Header.detector_roi_setting == 'Fixed',
                    Header.detector_roi_setting == 'Full Frame'))
        else:
            query = query.filter(Header.detector_roi_setting ==
                                 self['detector_roi'])

    if 'photstandard' in self:
        query = query.filter(Header.phot_standard == True)

    if 'twilight' in self:
        if self['twilight']:
            query = query.filter(Header.object == 'Twilight')
        else:
            query = query.filter(Header.object != 'Twilight')

    if 'az' in self:
        a, b = _parse_range(self['az'])
        if a is not None and b is not None:
            query = query.filter(Header.azimuth >= a).filter(Header.azimuth < b)
            query = querypropcoords(query)

    if 'el' in self:
        a, b = _parse_range(self['el'])
        if a is not None and b is not None:
            query = query.filter(Header.elevation >= a).\
                filter(Header.elevation < b)
            query = querypropcoords(query)

    # cosdec value is used in 'ra' code below to scale the search radius
    cosdec = None
    if 'dec' in self:
        valid = True
        # might be a range or a single value
        match = re.match(r"(-?[\d:\.]+)-(-?[\d:\.]+)", self['dec'])
        if match is None:
            # single value
            degs = gmu.dectodeg(self['dec'])
            if degs is None:
                # Invalid value.
                self['warning'] = 'Invalid Dec format. ' \
                                       'Ignoring your Dec constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(self.keys()):
                    sr = gmu.srtodeg(self['sr'])
                    if sr is None:
                        self['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        self['sr'] = '180'
                        sr = gmu.srtodeg(self['sr'])
                else:
                    # No search radius specified. Default it for them
                    self['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    self['sr'] = '180'
                    sr = gmu.srtodeg(self['sr'])
                lower = degs - sr
                upper = degs + sr

                # Also set cosdec value here for use in 'ra' code below
                cosdec = math.cos(math.radians(degs))

        else:
            # Got two values
            lower = gmu.dectodeg(match.group(1))
            upper = gmu.dectodeg(match.group(2))
            if (lower is None) or (upper is None):
                self['warning'] = 'Invalid Dec range format. ' \
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

    if 'ra' in self:
        valid = True
        # might be a range or a single value
        value = self['ra'].split('-')
        if len(value) == 1:
            # single value
            degs = gmu.ratodeg(value[0])
            if degs is None:
                # Invalid value.
                self['warning'] = 'Invalid RA format. ' \
                                       'Ignoring your RA constraint.'
                valid = False
            else:
                # valid single value, get search radius
                if 'sr' in list(self.keys()):
                    sr = gmu.srtodeg(self['sr'])
                    if sr is None:
                        self['warning'] = 'Invalid Search Radius, ' \
                                               'defaulting to 3 arcmin'
                        self['sr'] = '180'
                        sr = gmu.srtodeg(self['sr'])
                else:
                    # No search radius specified. Default it for them
                    self['warning'] = 'No Search Radius given, ' \
                                           'defaulting to 3 arcmin'
                    self['sr'] = '180'
                    sr = gmu.srtodeg(self['sr'])

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
                self['warning'] = 'Invalid RA range format. ' \
                                       'Ignoring your RA constraint.'
                valid = False

        else:
            # Invalid string format for RA
            self['warning'] = 'Invalid RA format. ' \
                                   'Ignoring your RA constraint.'
            valid = False

        if valid and (lower is not None) and (upper is not None):
            if upper > lower:
                query = query.filter(Header.ra >= lower).\
                    filter(Header.ra < upper)
            else:
                query = query.filter(or_(Header.ra >= lower, Header.ra < upper))
            query = querypropcoords(query)

    if 'exposure_time' in self:
        valid = True
        expt = None
        lower = None
        upper = None
        # might be a range or a single value
        self['exposure_time'] = self['exposure_time'].replace(' ', '')
        match = re.match(r"([\d\.]+)-([\d\.]+)", self['exposure_time'])
        if match is None:
            # single value
            try:
                expt = float(self['exposure_time'])
            except:
                pass
            if expt is None:
                # Invalid format
                self['warning'] = "Invalid format for exposure time, " \
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
                self['warning'] = 'Invalid format for exposure time ' \
                                       'range. Ignoring it.'
                valid = False

        if valid:
            query = query.filter(Header.exposure_time >= lower)\
                .filter(Header.exposure_time <= upper)

    if 'crpa' in self:
        a, b = _parse_range(self['crpa'])
        if a is not None and b is not None:
            query = query.filter(Header.cass_rotator_pa >= a)\
                .filter(Header.cass_rotator_pa < b)
            query = querypropcoords(query)

    if 'filepre' in self:
        likestr = '%s%%' % self['filepre']
        query = query.filter(File.name.like(likestr))

    if 'cenwlen' in self:
        valid = True
        # Might be a single value or a range
        value = self['cenwlen'].split('-')
        if len(value) == 1:
            # single value
            try:
                value = float(value[0])
                lower = value - 0.1
                upper = value + 0.1
            except:
                self['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        elif len(value) == 2:
            # Range
            try:
                lower = float(value[0])
                upper = float(value[1])
            except:
                self['warning'] = 'Central Wavelength value is invalid ' \
                                       'and has been ignored'
                valid = False
        else:
            self['warning'] = 'Central Wavelength value is invalid ' \
                                   'and has been ignored'
            valid = False

        if valid and not ((0.2 < lower < 30) and (0.2 < upper < 30)):
            self['warning'] = 'Invalid Central wavelength value. Value ' \
                                   'should be in microns, >0.2 and <30.0'
            if lower > upper:
                lower, upper = upper, lower
            if lower < 0.2:
                lower = 0.2
            if upper > 30:
                upper = 30
            if lower > 30 or upper < 0.2:
                # only reject the terms outright if they are out of range
                self['warning'] = 'Invalid Central wavelength value. ' \
                                       'Value should be in microns, >0.2 and ' \
                                       '<30.0 - Ignoring terms'
                valid = False

        if valid and (lower > upper):
            lower, upper = upper, lower

        if valid:
            query = query.filter(Header.central_wavelength > lower)\
                .filter(Header.central_wavelength < upper)

    if fsc.is_server and 'publication' in self:
        query = query.join(Program, Header.program_id == Program.program_id)\
            .join(ProgramPublication, Program.id == ProgramPublication.program_id)\
            .join(Publication, Publication.id == ProgramPublication.publication_id)\
            .filter(Publication.bibcode == self['publication'])

    if fsc.is_server and ('PIname' in self or 'ProgramText' in self):
        query = query.join(Program, Header.program_id == Program.program_id)
        if 'PIname' in self:
            query = query.filter(
                func.to_tsvector(Program.pi_coi_names)
                .match(' & '.join(self['PIname'].split()))
                )
        if 'ProgramText' in self:
            query = query.filter(
                func.to_tsvector(Program.title)
                .match(' & '.join(self['ProgramText'].split()))
                )

    if 'gpi_astrometric_standard' in self:
        query = query.join(Gpi, Gpi.header_id == Header.id)
        query = query.filter(Gpi.astrometric_standard ==
                             self['gpi_astrometric_standard'])

    if 'standard' in self:
        query = query.filter(Header.types.ilike('%''STANDARD''%'))

    if not ignore_processing_tag:
        if fsc.is_server and 'processing_tag' in self:
            if self['processing_tag'] == 'default':
                # Generate the list of processing_tags to include. This involves
                # a lookup on the processing_tags table
                tags = default_processing_tags(self)
                # If the processing tags list is empty, ignore it. This happens
                # when the results include legacy processed data that is has no
                # tag, or when the results only include raw data
                if len(tags):
                    # Search for data that is Raw, or has this tag. If there's
                    # also a selection on processing, that will apply too.
                    query = query.filter(or_(
                        Header.processing=='Raw', Header.processing_tag.in_(tags)))
                else:
                    # Simply disregard. If there's a selection on processing,
                    # that will apply regardless, no need to search on Raw here.
                    pass
            else:
                # Search for a specific processing tag
                # Search for data that is Raw, or has this tag.
                query = query.filter(or_(
                    Header.processing=='Raw',
                    Header.processing_tag==self['processing_tag']))
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
