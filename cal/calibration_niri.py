"""
This module holds the CalibrationNIRI class
"""
import datetime

from orm.diskfile import DiskFile
from orm.header import Header
from orm.niri import Niri
from cal.calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationNIRI(Calibration):
    """
    This class implements a calibration manager for NIRI.
    It is a subclass of Calibration
    """
    niri = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the niriheader
        if header:
            query = session.query(Niri).filter(Niri.header_id == self.descriptors['header_id'])
            self.niri = query.first()

        # Populate the descriptors dictionary for NIRI
        if self.from_descriptors:
            self.descriptors['data_section'] = self.niri.data_section
            self.descriptors['read_mode'] = self.niri.read_mode
            self.descriptors['well_depth_setting'] = self.niri.well_depth_setting
            self.descriptors['coadds'] = self.niri.coadds
            self.descriptors['filter_name'] = self.niri.filter_name
            self.descriptors['camera'] = self.niri.camera

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIRI dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK and FLAT
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')
            # No flats for L', M' Br(alpha) or Br(alpha) continuum as per AS 20130514
            if self.descriptors['filter_name'] not in ['Lprime_G0207', 'Mprime_G0208', 'Bra_G0238', 'Bracont_G0237']:
                self.applicable.append('flat')

    def dark(self, processed=False, many=None):
        query = self.session.query(Header).select_from(join(join(Niri, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')
        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
        else:
            query = query.filter(Header.reduction == 'RAW')

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: data_section, read_mode, well_depth_setting, exposure_time, coadds
        query = query.filter(Niri.data_section == self.descriptors['data_section'])
        query = query.filter(Niri.read_mode == self.descriptors['read_mode'])
        query = query.filter(Niri.well_depth_setting == self.descriptors['well_depth_setting'])

        # Exposure time must match to within 0.01 (nb floating point match). Coadds must also match.
        # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
        query = query.filter(Niri.coadds == self.descriptors['coadds'])
        exptime_lo = float(self.descriptors['exposure_time']) - 0.01
        exptime_hi = float(self.descriptors['exposure_time']) + 0.01
        query = query.filter(Header.exposure_time > exptime_lo).filter(Header.exposure_time < exptime_hi)

        # Absolute time separation must be within ~6 months
        max_interval = datetime.timedelta(days=180)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # We only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

    def flat(self, processed=False, many=None):
        query = self.session.query(Header).select_from(join(join(Niri, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')
        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
        else:
            query = query.filter(Header.reduction == 'RAW')

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: data_section, well_depth_setting, filter_name, camera
        # Update from AS 20130320 - read mode should not be required to match, but well depth should.
        query = query.filter(Niri.data_section == self.descriptors['data_section'])
        query = query.filter(Niri.well_depth_setting == self.descriptors['well_depth_setting'])
        query = query.filter(Niri.filter_name == self.descriptors['filter_name'])
        query = query.filter(Niri.camera == self.descriptors['camera'])

        # Absolute time separation must be within 6 months
        max_interval = datetime.timedelta(days=180)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # We only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()
