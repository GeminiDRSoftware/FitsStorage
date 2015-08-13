"""
This module holds the CalibrationNIRI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.niri import Niri
from .calibration import Calibration, not_processed

from sqlalchemy.orm import join
from sqlalchemy import func, extract, or_


class CalibrationNIRI(Calibration):
    """
    This class implements a calibration manager for NIRI.
    It is a subclass of Calibration
    """
    niri = None
    instrClass = Niri

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the niriheader
        if header:
            query = session.query(Niri).filter(Niri.header_id == self.descriptors['header_id'])
            self.niri = query.first()

        # Populate the descriptors dictionary for NIRI
        if self.from_descriptors:
            self.descriptors['data_section'] = self.niri.data_section
            self.descriptors['read_mode'] = self.niri.read_mode
            self.descriptors['well_depth_setting'] = self.niri.well_depth_setting
            self.descriptors['coadds'] = self.niri.coadds
            self.descriptors['filter_name'] = self.niri.filter_name
            self.descriptors['camera'] = self.niri.camera
            self.descriptors['focal_plane_mask'] = self.niri.focal_plane_mask
            self.descriptors['disperser'] = self.niri.disperser

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIRI dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK and FLAT, and photometric_standard
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')
            # No flats for L', M' Br(alpha) or Br(alpha) continuum as per AS 20130514
            if self.descriptors['filter_name'] not in ['Lprime_G0207', 'Mprime_G0208', 'Bra_G0238', 'Bracont_G0237']:
                self.applicable.append('flat')
            self.applicable.append('photometric_standard')

        # Imaging Lamp-on Flat fields require a lampoff_flat
        if (self.descriptors['observation_type'] == 'FLAT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['gcal_lamp'] != 'Off'):
            self.applicable.append('lampoff_flat')

        # Spectroscopy OBJECTs require a flat and arc
        if self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['spectroscopy'] == True:
            self.applicable.append('flat') 
            self.applicable.append('processed_flat') 
            self.applicable.append('arc') 
            # science Spectroscopy OBJECTs require a telluric
            if self.descriptors['observation_class'] == 'science':
                self.applicable.append('telluric_standard')

    def dark(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed)
                .match_descriptors(Niri.data_section,
                                   Niri.read_mode,
                                   Niri.well_depth_setting,
                                   Niri.coadds)
                # Exposure time must match to within 0.01 (nb floating point match). Coadds must also match.
                # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
                .tolerance(exposure_time = 0.01)
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .limit(howmany)
                .all()
            )

    def flat(self, processed=False, howmany=None):
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .flat(processed)
                # GCAL lamp should be on - these flats will then require lamp-off flats to calibrate them
                .add_filters(or_(Header.gcal_lamp == 'IRhigh', Header.gcal_lamp == 'IRlow'))
                # Must totally match: data_section, well_depth_setting, filter_name, camera, focal_plane_mask, disperser
                # Update from AS 20130320 - read mode should not be required to match, but well depth should.
                .match_descriptors(Niri.data_section,
                                   Niri.well_depth_setting,
                                   Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001, condition=self.descriptors['spectroscopy'])
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .limit(howmany)
                .all()
            )

    def arc(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed)
                .match_descriptors(Niri.data_section,
                                   Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001)
                # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .limit(howmany)
                .all()
            )

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                .flat()
                .add_filters(Header.gcal_lamp == 'Off')
                .match_descriptors(Niri.data_section,
                                   Niri.well_depth_setting,
                                   Niri.filter_name,
                                   Niri.camera,
                                   Niri.disperser)
                # Absolute time separation must be within 1 hour of the lamp on flats
                .max_interval(seconds=3600)
                .limit(howmany)
                .all()
            )

    @not_processed
    def photometric_standard(self, processed=False, howmany=None):
        # Default number to associate
        howmany = howmany if howmany else 10

        return (
            self.get_query()
                # Phot standards are OBJECT imaging frames
                .raw().OBJECT().spectroscopy(False)
                # Phot standards are phot standards
                .add_filters(Header.phot_standard == True)
                .match_descriptors(Niri.filter_name,
                                   Niri.camera)
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
                .teluric_standard(OBJECT=True, partnerCal=True)
                .match_descriptors(Niri.filter_name,
                                   Niri.camera,
                                   Niri.focal_plane_mask,
                                   Niri.disperser)
                .tolerance(central_wavelength = 0.001)
                # Absolute time separation must be within 24 hours of the science
                .max_interval(days=1)
                .limit(howmany)
                .all()
            )
