"""
This module holds the CalibrationGSAOI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gsaoi import Gsaoi
from .calibration import Calibration, not_processed

class CalibrationGSAOI(Calibration):
    """
    This class implements a calibration manager for GSAOI.
    It is a subclass of Calibration
    """
    instrClass = Gsaoi
    instrDescriptors = (
        'filter_name',
        'read_mode'
        )

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
            query = self.get_query().PROCESSED_FLAT()
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
