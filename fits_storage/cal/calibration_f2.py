"""
This module holds the CalibrationF2 class
"""

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
    instrClass = F2

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


    # TODO: Check with Paul if the semantics are right
    def dark(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed=processed)
                .match_descriptors(Header.exposure_time,
                                   F2.read_mode)
                # Must totally match: read_mode, exposure_time
                .max_interval(days=90)
                .limit(howmany)
                .all()
            )

        return query.all()

    @staticmethod
    def common_descriptors():
        return (F2.disperser, F2.lyot_stop, F2.filter_name, F2.focal_plane_mask)

    def flat(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed=processed)
                # Must totally match: disperser, central_wavelength (spect only), focal_plane_mask, filter_name, lyot_stop, read_mode
                .match_descriptors(F2.read_mode
                                   *CalibrationF2.common_descriptors())
                .tolerance(central_wavelenght=0.001, condition=self.descriptors['spectroscopy'])
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .limit(howmany)
                .all()
            )

    def arc(self, processed=False, howmany=None):
        # Default number to associate is 1
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed=processed)
                # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, lyot_stop
                .match_descriptors(*CalibrationF2.common_descriptors())
                .tolerance(central_wavelenght=0.001)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .limit(howmany)
                .all()
            )

    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Photometric standards are OBJECT imaging partnerCal frames
                .photometric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(F2.filter_name,
                                   F2.lyot_stop)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .limit(howmany)
                .all()
            )

    @not_processed
    def telluric_standard(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Telluric standards are OBJECT spectroscopy partnerCal frames
                .telluric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(*CalibrationF2.common_descriptors())
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .limit(howmany)
                .all()
            )
