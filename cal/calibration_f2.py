"""
This module holds the CalibrationF2 class
"""

import datetime

from orm.diskfile import DiskFile
from orm.header import Header
from orm.f2 import F2
from cal.calibration import Calibration

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
        Calibration.__init__(self, session, header, descriptors, types)

        # Find the f2header
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

        # Spectroscopy OBJECTs require a dark, flat and arc
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('dark')
            self.applicable.append('flat')
            self.applicable.append('arc')

        # FLAT frames require DARKs
        if self.descriptors['observation_type'] == 'FLAT':
            self.applicable.append('dark')

        # ARCs require DARKs and FLATs
        if self.descriptors['observation_type'] == 'ARC':
            self.applicable.append('dark')
            self.applicable.append('flat')


    def dark(self, processed=False, many=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, exposure_time
        query = query.filter(F2.read_mode == self.descriptors['read_mode'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])

        # Absolute time separation must be within 3 months
        max_interval = datetime.timedelta(days=90)
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
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength (spect only), focal_plane_mask, filter_name, lyot_stop, read_mode
        query = query.filter(F2.disperser == self.descriptors['disperser'])
        query = query.filter(F2.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])
        query = query.filter(F2.read_mode == self.descriptors['read_mode'])

        if self.descriptors['spectroscopy']:
            query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)

        # Absolute time separation must be within 3 months
        max_interval = datetime.timedelta(days=90)
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

    def arc(self, sameprog=False, many=None):
        query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
        query = query.filter(Header.observation_type == 'ARC')

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, lyot_stop
        query = query.filter(F2.disperser == self.descriptors['disperser'])
        query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)
        query = query.filter(F2.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(F2.filter_name == self.descriptors['filter_name'])
        query = query.filter(F2.lyot_stop == self.descriptors['lyot_stop'])

        # Absolute time separation must be within 3 months
        max_interval = datetime.timedelta(days=90)
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
