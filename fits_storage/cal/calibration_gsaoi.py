"""
This module holds the CalibrationGSAOI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gsaoi import Gsaoi
from .calibration import Calibration, not_processed

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationGSAOI(Calibration):
    """
    This class implements a calibration manager for GSAOI.
    It is a subclass of Calibration
    """
    gsaoi = None
    instrClass = Gsaoi

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
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().reduction('PROCESSED_FLAT')
        else:
            query = (self.get_query().raw().OBJECT()
                         .observation_class('dayCal')
                         # Notice that object() is observation_type=='OBJECT', in case the next confuses you...
                         .add_filters(Header.object == 'Domeflat'))

        return (
                # Common filter, with absolute time separation within a month
            query.match_descriptors(Gsaoi.filter_name)
                 .max_interval(days=30)
                 .limit(howmany)
                 .all()
            )

    # Processed photometric standards haven't been implemented
    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 8

        return (
            self.get_query()
                .raw().OBJECT().partnerCal()
                # Common filter, with absolute time separation within a month
                .match_descriptors(Gsaoi.filter_name)
                .max_interval(days=30)
                .limit(howmany)
                .all()
            )
