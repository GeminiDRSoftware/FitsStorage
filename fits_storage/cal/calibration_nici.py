"""
This module holds the CalibrationNICI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.nici import Nici
from .calibration import Calibration, not_processed

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationNICI(Calibration):
    """
    This class implements a calibration manager for NICI.
    It is a subclass of Calibration
    """
    nici = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the niciheader
        if header:
            query = session.query(Nici).filter(Nici.header_id == self.descriptors['header_id'])
            self.nici = query.first()

        # Populate the descriptors dictionary for NICI
        if self.from_descriptors:
            self.descriptors['filter_name'] = self.nici.filter_name
            self.descriptors['focal_plane_mask'] = self.nici.focal_plane_mask
            self.descriptors['disperser'] = self.nici.disperser

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NICI dataset
        self.applicable = []

        # Science OBJECTs require a DARK and FLAT
        if (self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')
            self.applicable.append('flat')

        # Lamp-on Flat fields require a lampoff_flat
        if (self.descriptors['observation_type'] == 'FLAT' and
                self.descriptors['gcal_lamp'] != 'Off'):
            self.applicable.append('lampoff_flat')


    def dark(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nici, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
            # Associate 1 processed dark by default
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Associate 10 raw darks by default
            howmany = howmany if howmany else 10

        # Exposure time must match to within 0.01 (nb floating point match).
        # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
        exptime_lo = float(self.descriptors['exposure_time']) - 0.01
        exptime_hi = float(self.descriptors['exposure_time']) + 0.01
        query = query.filter(Header.exposure_time > exptime_lo).filter(Header.exposure_time < exptime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 1 day
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=1), limit=howmany)

        return query.all()

    def flat(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nici, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number to associate
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Default number to associate
            howmany = howmany if howmany else 10

        # Must totally match: filter_name, focal_plane_mask, disperser
        query = query.filter(Nici.filter_name == self.descriptors['filter_name'])
        query = query.filter(Nici.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nici.disperser == self.descriptors['disperser'])

        # GCAL lamp should be on - these flats will then require lamp-off flats to calibrate them
        query = query.filter(Header.gcal_lamp == 'IRhigh')

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 1 day
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=1), limit=howmany)

        return query.all()

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nici, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')
        query = query.filter(Header.reduction == 'RAW')
        # Default number to associate
        howmany = howmany if howmany else 10

        # Must totally match: data_section, well_depth_setting, filter_name, camera
        # Update from AS 20130320 - read mode should not be required to match, but well depth should.
        query = query.filter(Nici.filter_name == self.descriptors['filter_name'])
        query = query.filter(Nici.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nici.disperser == self.descriptors['disperser'])

        # GCAL lamp should be off
        query = query.filter(Header.gcal_lamp == 'Off')

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 1 hour of the lamp on flats
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(seconds=3600), limit=howmany)

        return query.all()
