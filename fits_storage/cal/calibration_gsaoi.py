"""
This module holds the CalibrationGSAOI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gsaoi import Gsaoi
from .calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationGSAOI(Calibration):
    """
    This class implements a calibration manager for GSAOI.
    It is a subclass of Calibration
    """
    gsaoi = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # If header based, find the gsaoiheader
        if header:
            query = session.query(Gsaoi).filter(Gsaoi.header_id == self.descriptors['header_id'])
            self.gsaoi = query.first()

        # Populate the descriptors dictionary for GSAOI
        if self.from_descriptors:
            self.descriptors['filter_name'] = self.gsaoi.filter_name
            self.descriptors['read_mode'] = self.gsaoi.read_mode

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this GSAOI dataset
        self.applicable = []

        # Science OBJECTs require DomeFlats and photometric_standards
        if self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['observation_class'] == 'science':
            self.applicable.append('domeflat')
            self.applicable.append('photometric_standard')


    def domeflat(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Gsaoi, Header), DiskFile))

        if processed:
            # Default number to associate
            howmany = howmany if howmany else 1
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
        else:
            # Default number to associate
            howmany = howmany if howmany else 20
            query = query.filter(Header.reduction == 'RAW')
            query = query.filter(Header.observation_type == 'OBJECT')
            query = query.filter(Header.observation_class == 'dayCal')
            query = query.filter(Header.object == 'Domeflat')
 
        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: filter_name
        query = query.filter(Gsaoi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 month
        max_interval = datetime.timedelta(days=30)
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

    def photometric_standard(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Gsaoi, Header), DiskFile))

        if processed:
            # Not implemented
            return []
        else:
            # Default number to associate
            howmany = howmany if howmany else 8

        # They are partnerCal OBJECT frames
        query = query.filter(Header.reduction == 'RAW')
        query = query.filter(Header.observation_type == 'OBJECT')
        query = query.filter(Header.observation_class == 'partnerCal')
 
        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: filter_name
        query = query.filter(Gsaoi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 month
        max_interval = datetime.timedelta(days=30)
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

