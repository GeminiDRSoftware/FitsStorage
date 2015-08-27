"""
This module holds the CalibrationNIFS class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.nifs import Nifs
from .calibration import Calibration

class CalibrationNIFS(Calibration):
    """
    This class implements a calibration manager for NIFS.
    It is a subclass of Calibration
    """
    instrClass = Nifs
    instrDescriptors = (
        'read_mode',
        'disperser',
        'focal_plane_mask',
        'filter_name',
        )

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIFS dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')

        # Science spectroscopy that is not a progcal or partnercal requires a flat, arc, ronchi_mask and telluric_standard
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['observation_class'] not in ['partnerCal', 'progCal'] and
                self.descriptors['spectroscopy'] == True):
            self.applicable.append('flat')
            self.applicable.append('processed_flat')
            self.applicable.append('arc')
            self.applicable.append('ronchi_mask')
            self.applicable.append('telluric_standard')

        # Flats require lampoff_flats
        if self.descriptors['observation_type'] == 'FLAT' and self.descriptors['gcal_lamp'] != 'Off':
            self.applicable.append('lampoff_flat')

    @staticmethod
    def common_descriptors():
        return (Header.central_wavelength, Nifs.disperser, Nifs.focal_plane_mask, Nifs.filter_name)

    def dark(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed)
                .match_descriptors(Header.exposure_time,
                                   Nifs.read_mode,
                                   Header.coadds,
                                   Nifs.disperser)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def flat(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed)
                # GCAL lamp must be IRhigh or QH
                .add_filters(Header.gcal_lamp.in_(['IRhigh', 'QH']))
                # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
                .match_descriptors(*CalibrationNIFS.common_descriptors())
                # Absolute time separation must be within 10 days
                .max_interval(days=10)
                .all(howmany)
            )

    def lampoff_flat(self, howmany=None):
        # Default number of processed flats to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                .flat()
                # GCAL lamp must be IRhigh or QH
                .add_filters(Header.gcal_lamp == 'Off')
                # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
                .match_descriptors(*CalibrationNIFS.common_descriptors())
                # Absolute time separation must be within 1 hour
                .max_interval(seconds=3600)
                .all(howmany)
            )

    def arc(self, howmany=None):
        # Always associate 1 arc by default
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc()
                .match_descriptors(*CalibrationNIFS.common_descriptors())
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def ronchi_mask(self, processed=False, howmany=None):
        # Always associate 1 ronchi by default
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .observation_type('RONCHI')
                .match_descriptors(Header.central_wavelength,
                                   Nifs.disperser)
                # NOTE: No max interval?
                .all(howmany)
            )

    def telluric_standard(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 12

        return (
            self.get_query()
                # Telluric standards are OBJECT spectroscopy partnerCal frames
                .telluric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(*CalibrationNIFS.common_descriptors())
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )
