from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy import Numeric, Boolean, Date
from sqlalchemy import Time, BigInteger, Enum

from sqlalchemy.orm import relation
from . import Base
from .diskfile import DiskFile

from fits_storage.logger import DummyLogger
from fits_storage.file_parser import build_parser
from fits_storage.gemini_metadata_utils import GeminiProgram, procmode_codes
from fits_storage.gemini_metadata_utils import gemini_gain_settings
from fits_storage.gemini_metadata_utils import gemini_readspeed_settings
from fits_storage.gemini_metadata_utils import gemini_welldepth_settings

from astropy import wcs as pywcs
from astropy.wcs import SingularMatrixError
from astropy.io import fits


__all__ = ["Header"]

from fits_storage.gemini_metadata_utils import obs_types, obs_classes, \
    reduction_states


# Enumerated Column types
PROCMODE_ENUM = Enum(*procmode_codes, name='procmode')
OBSTYPE_ENUM = Enum(*obs_types, name='obstype')
OBSCLASS_ENUM = Enum(*obs_classes, name='obsclass')
REDUCTION_STATE_ENUM = Enum(*reduction_states, name='reduction_state')
TELESCOPE_ENUM = Enum('Gemini-North', 'Gemini-South', name='telescope')
QASTATE_ENUM = Enum('Fail', 'CHECK', 'Undefined', 'Usable', 'Pass', name='qa_state')
MODE_ENUM = Enum('imaging', 'spectroscopy', 'LS', 'MOS', 'IFS', 'IFP', name='mode')
DETECTOR_GAIN_ENUM = Enum('None', *gemini_gain_settings, name='detector_gain_setting')
DETECTOR_READSPEED_ENUM = Enum('None', *gemini_readspeed_settings, name='detector_readspeed_setting')
DETECTOR_WELLDEPTH_ENUM = Enum('None', *gemini_welldepth_settings, name='detector_welldepth_setting')


# ------------------------------------------------------------------------------
class Header(Base):
    """
    This is the ORM class for the Header table.

    Parameters
    ----------
    diskfile : :class:`~fits_storage_core.orm.diskfile.DiskFile`
        The file this header is taken from
    """
    __tablename__ = 'header'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False,
                         index=True)
    diskfile = relation(DiskFile, order_by=id)
    program_id = Column(Text, index=True)
    engineering = Column(Boolean, index=True)
    science_verification = Column(Boolean, index=True)
    calibration_program = Column(Boolean, index=True)
    procmode = Column(PROCMODE_ENUM)
    observation_id = Column(Text, index=True)
    data_label = Column(Text, index=True)
    telescope = Column(TELESCOPE_ENUM, index=True)
    instrument = Column(Text, index=True)
    ut_datetime = Column(DateTime(timezone=False), index=True)
    ut_datetime_secs = Column(BigInteger, index=True)
    local_time = Column(Time(timezone=False))
    observation_type = Column(OBSTYPE_ENUM, index=True)
    observation_class = Column(OBSCLASS_ENUM, index=True)
    object = Column(Text, index=True)
    ra = Column(Numeric(precision=16, scale=12), index=True)
    dec = Column(Numeric(precision=16, scale=12), index=True)
    azimuth = Column(Numeric(precision=16, scale=12))
    elevation = Column(Numeric(precision=16, scale=12))
    cass_rotator_pa = Column(Numeric(precision=16, scale=12))
    airmass = Column(Numeric(precision=8, scale=6))
    filter_name = Column(Text, index=True)
    exposure_time = Column(Numeric(precision=8, scale=4))
    disperser = Column(Text, index=True)
    camera = Column(Text, index=True)
    central_wavelength = Column(Numeric(precision=8, scale=6), index=True)
    wavelength_band = Column(Text)
    focal_plane_mask = Column(Text, index=True)
    pupil_mask = Column(Text, index=True)
    detector_binning = Column(Text)
    detector_roi_setting = Column(Text)
    detector_gain_setting = Column(DETECTOR_GAIN_ENUM)
    detector_readspeed_setting = Column(DETECTOR_READSPEED_ENUM)
    detector_welldepth_setting = Column(DETECTOR_WELLDEPTH_ENUM)
    detector_readmode_setting = Column(Text)
    coadds = Column(Integer)
    spectroscopy = Column(Boolean, index=True)
    mode = Column(MODE_ENUM, index=True)
    adaptive_optics = Column(Boolean)
    laser_guide_star = Column(Boolean)
    wavefront_sensor = Column(Text)
    gcal_lamp = Column(Text)
    raw_iq = Column(Integer)
    raw_cc = Column(Integer)
    raw_wv = Column(Integer)
    raw_bg = Column(Integer)
    requested_iq = Column(Integer)
    requested_cc = Column(Integer)
    requested_wv = Column(Integer)
    requested_bg = Column(Integer)
    qa_state = Column(QASTATE_ENUM, index=True)
    release = Column(Date)
    reduction = Column(REDUCTION_STATE_ENUM, index=True)
    # added per Trac #264, Support for Gemini South All Sky Camera
    site_monitoring = Column(Boolean)
    types = Column(Text)
    phot_standard = Column(Boolean)
    proprietary_coordinates = Column(Boolean)
    pre_image = Column(Boolean)

    def __init__(self, diskfile, logger=DummyLogger()):
        self.diskfile_id = diskfile.id
        self.populate_fits(diskfile, log=logger)

    def __repr__(self):
        return "<Header('%s', '%s')>" % (self.id, self.diskfile_id)

    def populate_fits(self, diskfile: DiskFile, log=DummyLogger()):
        """
        Populates header table values from the FITS headers of the file.
        Uses the AstroData object to access the file.

        Parameters
        ----------
        diskfile : :class:`~fits_storage_core.orm.diskfile.DiskFile`
            DiskFile record to read to populate :class:`~Header` record
        log : :class:`logging.Logger` or
            :class:`fits_storage.logger.DummyLogger`
            Logger to log messages to
        """

        parser = build_parser(diskfile.get_ad_object, log)

        # Check for site_monitoring data. Currently, this only comprises
        # GS_ALLSKYCAMERA, but may accommodate other monitoring data.
        self.site_monitoring = parser.site_monitoring()

        # Basic data identification section
        # Parse Program ID
        self.program_id = parser.program_id()
        if self.program_id is not None:
            # Set eng and sv booleans
            gemprog = GeminiProgram(self.program_id)
            self.engineering = gemprog.is_eng or not gemprog.valid
            self.science_verification = bool(gemprog.is_sv)
            self.calibration_program = bool(gemprog.is_cal)
        else:
            # program ID is None - mark as engineering
            self.engineering = True
            self.science_verification = False

        self.procmode = parser.procmode()
        self.observation_id = parser.observation_id()
        self.data_label = parser.data_label()
        self.telescope = parser.telescope()
        self.instrument = parser.instrument()

        # Date and times part
        self.ut_datetime = parser.ut_datetime()
        self.ut_datetime_secs = parser.ut_datetime_secs()
        self.local_time = parser.local_time()

        # Data Types
        self.observation_type = parser.observation_type()
        self.observation_class = parser.observation_class()

        self.object = parser.object()

        self.ra = parser.ra()
        self.dec = parser.dec()

        self.azimuth = parser.azimuth()
        self.elevation = parser.elevation()
        self.cass_rotator_pa = parser.cass_rotator_pa()
        self.airmass = parser.airmass()

        self.raw_iq = parser.raw_iq()
        self.raw_cc = parser.raw_cc()
        self.raw_wv = parser.raw_wv()
        self.raw_bg = parser.raw_bg()

        self.requested_iq = parser.requested_iq()
        self.requested_cc = parser.requested_cc()
        self.requested_wv = parser.requested_wv()
        self.requested_bg = parser.requested_bg()

        self.filter_name = parser.filter_name()
        self.exposure_time = parser.exposure_time()
        self.disperser = parser.disperser()
        self.camera = parser.camera()
        self.central_wavelength = parser.central_wavelength()
        self.wavelength_band = parser.wavelength_band()
        self.focal_plane_mask = parser.focal_plane_mask()
        self.pupil_mask = parser.pupil_mask()

        self.detector_binning = parser.detector_binning()
        self.detector_gain_setting = parser.gain_setting()
        self.detector_readspeed_setting = parser.read_speed_setting()
        self.detector_welldepth_setting = parser.well_depth_setting()
        self.detector_readmode_setting = parser.read_mode()
        self.detector_roi_setting = parser.detector_roi_setting()
        self.coadds = parser.coadds()
        self.adaptive_optics = parser.adaptive_optics()
        self.laser_guide_star = parser.laser_guide_star()
        self.wavefront_sensor = parser.wavefront_sensor()

        # And the Spectroscopy and mode items
        self.spectroscopy = parser.spectroscopy()
        self.mode = parser.mode()

        self.qa_state = parser.qa_state()
        self.release = parser.release()
        self.proprietary_coordinates = parser.proprietary_coordinates()
        self.gcal_lamp = parser.gcal_lamp()
        self.reduction = parser.reduction()
        self.pre_image = parser.pre_image()

        # Get the types list
        self.types = str(diskfile.ad_object.tags) \
            if hasattr(diskfile.ad_object, 'tags') else None

        return