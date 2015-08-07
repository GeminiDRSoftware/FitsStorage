"""
This module holds the CalibrationF2 class
"""

import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.f2 import F2
from .calibration import Calibration, not_processed

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationF2(Calibration):
    """
    This class implements a calibration manager for F2.
    It is a subclass of Calibration
    """
    f2 = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        """
        This is the init method for the F2 calibration subclass.
        """

        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the f2header
        if header:
            query = session.query(F2).filter(F2.header_id == self.descriptors['header_id'])
            self.f2 = query.first()

        # Populate the descriptors dictionary for F2
        if self.from_descriptors:
            self.descriptors['read_mode'] = self.f2.read_mode
            self.descriptors['disperser'] = self.f2.disperser
            self.descriptors['focal_plane_mask'] = self.f2.focal_plane_mask
            self.descriptors['filter_name'] = self.f2.filter_name
            self.descriptors['lyot_stop'] = self.f2.lyot_stop

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this dataset
        self.applicable = []

        # Imaging OBJECTs require a DARK and a flat except acq images
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] not in ['acq', 'acqCal']):
            self.applicable.append('dark')
            self.applicable.append('flat')
            # And if they're science frames, they require a photometric_standard
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('photometric_standard')

        # Spectroscopy OBJECTs require a dark, flat and arc
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('dark')
            self.applicable.append('flat')
            self.applicable.append('arc')
            # And if they're science frames, they require a telluric_standard
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('telluric_standard')

        # FLAT frames require DARKs
        if self.descriptors['observation_type'] == 'FLAT':
            self.applicable.append('dark')

        # ARCs require DARKs and FLATs
        if self.descriptors['observation_type'] == 'ARC':
            self.applicable.append('dark')
            self.applicable.append('flat')


    def dark(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        if processed:
            query.filter(Header.reduction == 'PROCESSED_DARK')
            # Default number of processed darks to associate
            howmany = howmany if howmany else 1
        else:
            query.filter(Header.reduction == 'RAW')
            # Default number of raw darks to associate
            howmany = howmany if howmany else 10

        # Must totally match: read_mode, exposure_time
        query = query.filter(F2.read_mode == self.descriptors['read_mode'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 3 months
        query = self.set_common_cals_filter(filter, max_interval=datetime.timedelta(days=90), limit=howmany)

        return query.all()

    def flat(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number of processed flats
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Default number of raw flats
            howmany = howmany if howmany else 10

        # Must totally match: disperser, central_wavelength (spect only), focal_plane_mask, filter_name, lyot_stop, read_mode
        query = query.filter(F2.disperser == self.descriptors['disperser'])
        query = query.filter(F2.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])
        query = query.filter(F2.read_mode == self.descriptors['read_mode'])

        if self.descriptors['spectroscopy']:
            query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 3 months
        query = self.set_common_cals_filter(filter, max_interval=datetime.timedelta(days=90), limit=howmany)

        return query.all()

    def arc(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'ARC')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_ARC')
        else:
            query = query.filter(Header.reduction == 'RAW')

        # Default number to associate is 1
        howmany = howmany if howmany else 1

        # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, lyot_stop
        query = query.filter(F2.disperser == self.descriptors['disperser'])
        query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)
        query = query.filter(F2.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 3 months
        query = self.set_common_cals_filter(filter, max_interval=datetime.timedelta(days=90), limit=howmany)

        return query.all()

    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))

        query = query.filter(Header.reduction == 'RAW')
        # Default number to associate
        howmany = howmany if howmany else 10

        # Phot standards are OBJECT imaging frames
        query = query.filter(Header.observation_type == 'OBJECT')
        query = query.filter(Header.spectroscopy == False)

        # Phot standards are partnerCals
        query = query.filter(Header.observation_class == 'partnerCal')

        # Must match filter and lyot stop
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 24 hours of the science
        query = self.set_common_cals_filter(filter, max_interval=datetime.timedelta(days=1), limit=howmany)

        return query.all()

    @not_processed
    def telluric_standard(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.reduction == 'RAW')

        # Default number to associate
        howmany = howmany if howmany else 10

        # Telluric standards are OBJECT spectroscopy partnerCal frames
        query = query.filter(Header.observation_type == 'OBJECT')
        query = query.filter(Header.spectroscopy == True)
        query = query.filter(Header.observation_class == 'partnerCal')

        # Must match filter, lyot_stop, focal_plane_mask, disperser
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])
        query = query.filter(F2.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(F2.disperser == self.descriptors['disperser'])

        # Central Wavelength must match within tollerance
        # Occassionally we get a None, so run this in a try except
        try:
            cenwlen_lo = float(self.descriptors['central_wavelength']) - 0.001
            cenwlen_hi = float(self.descriptors['central_wavelength']) + 0.001
            query = query.filter(Header.central_wavelength > cenwlen_lo).filter(Header.central_wavelength < cenwlen_hi)
        except TypeError:
            pass

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        # Absolute time separation must be within 24 hours of the science
        query = self.set_common_cals_filter(filter, max_interval=datetime.timedelta(days=1), limit=howmany)

        return query.all()
