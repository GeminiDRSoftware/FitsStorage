"""
This module holds the CalibrationMICHELLE class
"""
import datetime

from orm.diskfile import DiskFile
from orm.header import Header
from orm.michelle import Michelle
from cal.calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationMICHELLE(Calibration):
    """
    This class implements a calibration manager for MICHELLE.
    It is a subclass of Calibration
    """
    michelle = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # If header based, find the michelleheader
        if header:
            query = session.query(Michelle).filter(Michelle.header_id == self.descriptors['header_id'])
            self.michelle = query.first()

        # Populate the descriptors dictionary for MICHELLE
        if self.from_descriptors:
            self.descriptors['read_mode'] = self.michelle.read_mode
            self.descriptors['coadds'] = self.michelle.coadds
            self.descriptors['disperser'] = self.michelle.disperser
            self.descriptors['filter_name'] = self.michelle.filter_name
            self.descriptors['focal_plane_mask'] = self.michelle.focal_plane_mask

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this MICHELLE dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')

        # Science spectroscopy OBJECTs require a FLAT
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == True and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('flat')


    def dark(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Michelle, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        # Default number to associate
        howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, exposure_time, coadds
        query = query.filter(Michelle.read_mode == self.descriptors['read_mode'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])
        query = query.filter(Michelle.coadds == self.descriptors['coadds'])

        # Absolute time separation must be within 1 day
        max_interval = datetime.timedelta(days=1)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

    def flat(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Michelle, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        # Default number to associate
        howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, filter
        query = query.filter(Michelle.read_mode == self.descriptors['read_mode'])
        query = query.filter(Michelle.filter_name == self.descriptors['filter_name'])

        # If spectroscopy
        if self.descriptors['spectroscopy'] == True:
            # must match disperser, focal_plane_mask
            query = query.filter(Michelle.disperser == self.descriptors['disperser'])
            query = query.filter(Michelle.focal_plane_mask == self.descriptors['focal_plane_mask'])

            # Wavelength must match to within 0.001 um
            tolerance = 0.001
            wlen_lo = float(self.descriptors['central_wavelength']) - tolerance
            wlen_hi = float(self.descriptors['central_wavelength']) + tolerance
            query = query.filter(Header.central_wavelength < wlen_hi).filter(Header.central_wavelength > wlen_lo)

        # Absolute time separation must be within 1 day
        max_interval = datetime.timedelta(days=1)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

