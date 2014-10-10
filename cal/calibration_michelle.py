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

        # Find the michelleheader
        query = session.query(Michelle).filter(Michelle.header_id == self.descriptors['header_id'])
        self.michelle = query.first()

        # Populate the descriptors dictionary for MICHELLE
        if self.from_descriptors:
            self.descriptors['read_mode'] = self.michelle.read_mode
            self.descriptors['coadds'] = self.michelle.coadds

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


    def dark(self, processed=False, many=None):
        query = self.session.query(Header).select_from(join(join(Michelle, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, exposure_time, coadds
        query = query.filter(Michelle.read_mode == self.descriptors['read_mode'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])
        query = query.filter(Michelle.coadds == self.descriptors['coadds'])

        # Absolute time separation must be within 1 year
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation
        query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()
