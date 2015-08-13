"""
This module holds the CalibrationNICI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.nici import Nici
from .calibration import Calibration, not_processed

class CalibrationNICI(Calibration):
    """
    This class implements a calibration manager for NICI.
    It is a subclass of Calibration
    """
    instrClass = Nici
    instrDescriptors = (
        'filter_name',
        'focal_plane_mask',
        'disperser'
        )

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
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed)
                # Exposure time must match to within 0.01 (nb floating point match).
                # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
                .tolerance(exposure_time = 0.01)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )

    def flat(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed)
                # GCAL lamp should be on - these flats will then require lamp-off flats to calibrate them
                .add_filters(Header.gcal_lamp == 'IRhigh')
                .match_descriptors(Nici.filter_name,
                                   Nici.focal_plane_mask,
                                   Nici.disperser)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                .flat()
                .add_filters(Header.gcal_lamp == 'Off')
                # NOTE: check this comment...
                # Must totally match: data_section, well_depth_setting, filter_name, camera
                # Update from AS 20130320 - read mode should not be required to match, but well depth should.
                .match_descriptors(Nici.filter_name,
                                   Nici.focal_plane_mask,
                                   Nici.disperser)
                # Absolute time separation must be within 1 hour of the lamp on flats
                .max_interval(seconds=3600)
                .all(howmany)
            )
