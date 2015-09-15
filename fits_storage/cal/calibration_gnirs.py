"""
This module holds the CalibrationGNIRS class
"""
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gnirs import Gnirs
from .calibration import Calibration, not_processed

class CalibrationGNIRS(Calibration):
    """
    This class implements a calibration manager for GNIRS.
    It is a subclass of Calibration
    """
    instrClass = Gnirs
    instrDescriptors = (
        'read_mode',
        'well_depth_setting',
        'disperser',
        'focal_plane_mask',
        'camera',
        'filter_name'
        )

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GNIRS frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        # Science Imaging OBJECTs that are not acq or acqCal require a DARK and a FLAT
        if ((self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal']) and
                (self.descriptors['spectroscopy'] == False)):
            self.applicable.append('dark')
            self.applicable.append('flat')
            self.applicable.append('lampoff_flat')
            self.applicable.append('processed_flat')

        # Spectroscopy OBJECT frames require a flat and arc and telluric_standard
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('flat')
            self.applicable.append('lampoff_flat')
            self.applicable.append('arc')
            self.applicable.append('pinhole_mask')
            # and if they are XD, they need a Quartz-Halogen flat (qh_flat) too.
            if 'XD' in self.descriptors['disperser']:
                self.applicable.append('qh_flat')
            self.applicable.append('telluric_standard')

        # IR lamp-on flats can use lamp-off flats
        if self.descriptors['observation_type'] == 'FLAT' and self.descriptors['gcal_lamp'] == 'IRhigh':
            self.applicable.append('lampoff_flat')

    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS Dark for this target frame
        """
        # Default number of processed darks to associate
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_query()
                .dark(processed=processed)
                # Must totally match: read_mode, well_depth_setting, exposure_time, coadds
                .match_descriptors(Header.exposure_time,
                                   Gnirs.read_mode,
                                   Gnirs.well_depth_setting,
                                   Header.coadds)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def get_gnirs_flat_query(self, processed):
        return (
            self.get_query()
            .flat(processed=processed)
            # Must totally match: disperser, central_wavelength, focal_plane_mask, camera, filter_name, well_depth_setting
            # update from RM 20130321 - read mode should not be required to match, but well depth should.
            # For imaging, central wavelength and disperser are not required to match
            .match_descriptors(Gnirs.disperser,
                               Gnirs.focal_plane_mask,
                               Gnirs.camera,
                               Gnirs.filter_name,
                               Gnirs.well_depth_setting)
            .if_(self.descriptors['spectroscopy'], 'match_descriptors', Gnirs.disperser)
            .if_(self.descriptors['spectroscopy'], 'tolerance', central_wavelength=0.001)
        )

    def flat(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS IR flat field for this target frame
        """
        # GNIRS mostly uses GCAL flats with the IRhigh lamp.
        # Sometimes, eg thermal wavelengths, it just uses GCAL with the lamp off (shutter closed),
        # and the thermal background of gcal is adequate.
        # But also it sometimes (eg some imaging) uses a lamp-on flat and wants to substract a lamp-off flat
        # And then also in XD modes, it also wants a QH lamp flat for the shorter wavelengths.
        #
        # So, this cal association will give you either IRhigh flats. In some cases, we make
        # a lamp-off flat applicable to the lamp-on flat to give the subtraciton pairs. 
        # We also have lamp-off flats directly applicable to the science at thermal wavelengths.
        # and we consider QH flats a separate thing.

        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_gnirs_flat_query(processed)
                # Lamp selection (see comments above)
                .add_filters(Header.gcal_lamp == 'IRhigh')
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS ARC for this target frame
        """
        # Always default to 1 arc
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                .arc(processed=processed)
                # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, camera
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.focal_plane_mask,
                                   Gnirs.filter_name,
                                   Gnirs.camera)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def pinhole_mask(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS pinhole_mask for this target frame
        """
        if howmany is None:
            howmany = 1 if processed else 5

        return (
            self.get_query()
                .pinhole(processed)
                # Must totally match: disperser, central_wavelength, camera, (only for cross dispersed mode?)
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.camera)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None):
        """
        Find the optimal lamp-off flats to go with the lamp-on flat
        """
        # Default number of raw lampoff flats
        howmany = howmany if howmany else 10

        return (
            self.get_gnirs_flat_query(processed=False) # lampoff flats are just Raw flats...
                .add_filters(Header.gcal_lamp == 'Off')
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )

    def qh_flat(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS QH flat field for this target frame
        """
        # GNIRS mostly uses GCAL flats with the IRhigh lamp.
        # Sometimes, eg thermal wavelengths, it just uses GCAL with the lamp off (shutter closed),
        # and the thermal background of gcal is adequate.
        # But also it sometimes (eg some imaging) uses a lamp-on flat and wants to substract a lamp-off flat
        # And then also in XD modes, it also wants a QH lamp flat for the shorter wavelengths.
        #
        # So, this cal association will give you either IRhigh flats or lamp-Off flats. In some cases, we make
        # a lamp-off flat applicable to the lamp-on flat to give the subtraciton pairs. 
        # and we consider QH flats a separate thing.
        if howmany is None:
            howmany = 1 if processed else 10

        return (
            self.get_gnirs_flat_query(processed) # QH flats are just flats...
                # ... with QH GCAL lamp
                .add_filters(Header.gcal_lamp == 'QH')
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def telluric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS telluric observations for this target frame
        """
        if howmany is None:
            howmany = 1 if processed else 8

        return (
            self.get_query()
                .telluric_standard(processed=processed, OBJECT=True, partnerCal=True)
                # Must totally match: disperser, central_wavelength, focal_plane_mask, camera, filter_name
                .match_descriptors(Header.central_wavelength,
                                   Gnirs.disperser,
                                   Gnirs.focal_plane_mask,
                                   Gnirs.camera,
                                   Gnirs.filter_name)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
                .all(howmany)
            )
