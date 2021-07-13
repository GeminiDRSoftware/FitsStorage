from sqlalchemy import Column, ForeignKey, String
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy import Numeric, Boolean, Date
from sqlalchemy import Time, BigInteger, Enum

from sqlalchemy.orm import relation

import numpy as np

import dateutil.parser
import datetime
import types

from . import Base
from .diskfile import DiskFile

from .utils.gemini_metadata_utils import GeminiProgram, procmode_codes, gemini_procmode

from .utils.gemini_metadata_utils import ratodeg
from .utils.gemini_metadata_utils import dectodeg
from .utils.gemini_metadata_utils import dmstodeg
from .utils.gemini_metadata_utils import gemini_observation_type
from .utils.gemini_metadata_utils import gemini_telescope
from .utils.gemini_metadata_utils import gemini_observation_class
from .utils.gemini_metadata_utils import gemini_instrument
from .utils.gemini_metadata_utils import gemini_gain_settings
from .utils.gemini_metadata_utils import gemini_readspeed_settings
from .utils.gemini_metadata_utils import gemini_welldepth_settings
from .utils.gemini_metadata_utils import gemini_readmode_settings
from .utils.gemini_metadata_utils import site_monitor

from astropy import wcs as pywcs
from astropy.wcs import SingularMatrixError
from astropy.io import fits


__all__ = ["Header"]


# This is a lingering circular dependency on DRAGONS
import astrodata               # For astrodata errors
import gemini_instruments
try:
    import ghost_instruments
except:
    pass

from .utils.gemini_metadata_utils import obs_types, obs_classes, reduction_states


# ------------------------------------------------------------------------------
# Replace spaces etc in the readmodes with _s
gemini_readmode_settings = [i.replace(' ', '_') for i in gemini_readmode_settings]

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

REDUCTION_STATUS = {
    'FLAT': 'PROCESSED_FLAT',
    'BIAS': 'PROCESSED_BIAS',
    'FRINGE': 'PROCESSED_FRINGE',
    'DARK': 'PROCESSED_DARK',
    'ARC': 'PROCESSED_ARC',
    'SCIENCE': 'PROCESSED_SCIENCE',
    'STANDARD': 'PROCESSED_STANDARD',
    'SLITILLUM': 'PROCESSED_SLITILLUM',
}


def _extract_zorro_wcs(ad):
    ctype1 = ad.phu.get('CTYPE1')
    ctype2 = ad.phu.get('CTYPE2')
    crval1 = ad.phu.get('CRVAL1')
    crval2 = ad.phu.get('CRVAL2')
    return ctype1, ctype2, crval1, crval2


def _ra_for_zorro(ad):
    try:
        return ad.ra()
    except:
        ctype1, ctype2, crval1, crval2 = _extract_zorro_wcs(ad)
        if ctype1 == 'RA---TAN' or ctype1 == 'RA--TAN':  # Zorro sometimes is broken with RA--TAN
            return crval1
        if ctype2 == 'RA---TAN' or ctype2 == 'RA--TAN':  # Zorro sometimes is broken with RA--TAN
            return crval2


def _dec_for_zorro(ad):
    try:
        return ad.dec()
    except:
        ctype1, ctype2, crval1, crval2 = _extract_zorro_wcs(ad)
        if ctype1 == 'DEC--TAN':
            return crval1
        if ctype2 == 'DEC--TAN':
            return crval2
    return None


_wcs_fns = {"zorro": (_ra_for_zorro, _dec_for_zorro),
            "alopeke": (_ra_for_zorro, _dec_for_zorro)}


def _ra(ad):
    try:
        instr = ad.instrument().lower()
        if instr in _wcs_fns:
            return _wcs_fns[instr][0](ad)
    except:
        pass  # fallback to ra()
    return ad.ra()


def _dec(ad):
    try:
        instr = ad.instrument().lower()
        if instr in _wcs_fns:
            return _wcs_fns[instr][1](ad)
    except:
        pass  # fallback to dec()
    return ad.dec()


def _try_or_none(fn, log, message):
    """
    Helper wrapper to try accessing a field on an AstroData instance
    but gracefully handle failures as None values.

    This covers a variety of issues ultimately due to bad headers in the FITS
    files.  We would rather get the file ingested with what data can be
    parsed than fail completely.

    Parameters
    ----------
    fn : str
        field name
    log : Logger
        logging instance
    message : str
        Message to log as a warning on failure

    Returns
    -------
    Value for the field, or None on error
    """
    try:
        retval = fn()
        return retval
    except (TypeError, AttributeError, KeyError, ValueError, IndexError) as tonerr:
        if log:
            log.warning(message)
        return None


# ------------------------------------------------------------------------------
class Header(Base):
    """
    This is the ORM class for the Header table.

    """
    __tablename__ = 'header'

    
    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
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

    def __init__(self, diskfile, log=None):
        self.diskfile_id = diskfile.id
        self.populate_fits(diskfile, log)

    def __repr__(self):
        return "<Header('%s', '%s')>" % (self.id, self.diskfile_id)

    UT_DATETIME_SECS_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)

    def populate_fits(self, diskfile, log=None):
        """
        Populates header table values from the FITS headers of the file.
        Uses the AstroData object to access the file.

        Parameters
        ----------
        diskfile : :class:`~diskfile.DiskFile`
            DiskFile record to read to populate :class:`~Header` record
        log : :class:`logging.Logger`
            Logger to log messages to
        """
        # The header object is unusual in that we directly pass the constructor
        # a diskfile object which may have an ad_object in it.
        if diskfile.ad_object is not None:
            ad = diskfile.ad_object
        else:
            if diskfile.uncompressed_cache_file:
                fullpath = diskfile.uncompressed_cache_file
            else:
                fullpath = diskfile.fullpath()
            ad = astrodata.open(fullpath)

        # Check for site_monitoring data. Currently, this only comprises
        # GS_ALLSKYCAMERA, but may accommodate other monitoring data.
        self.site_monitoring = site_monitor(ad.instrument())

        # Basic data identification section
        # Parse Program ID
        try:
            self.program_id = ad.program_id()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ae:
            if log:
                log.warn("Unable to parse program ID from datafile: %s" % ae)
            self.program_id = None
        if self.program_id is not None:
            # Ensure upper case
            self.program_id = self.program_id.upper()
            # Set eng and sv booleans
            gemprog = GeminiProgram(self.program_id)
            self.engineering = gemprog.is_eng or not gemprog.valid
            self.science_verification = bool(gemprog.is_sv)
            self.calibration_program = bool(gemprog.is_cal)
        else:
            # program ID is None - mark as engineering
            self.engineering = True
            self.science_verification = False

        try:
            self.procmode = gemini_procmode(ad.phu.get('PROCMODE'))
        except AttributeError as pmodeae:
            self.procmode = None
        if self.procmode is None:
            # check if PROCSCI is ql or sq for legacy file support
            try:
                self.procmode = gemini_procmode(ad.phu.get('PROCSCI'))
            except AttributeError as psciae:
                self.procmode = None
        try:
            self.observation_id = ad.observation_id()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as oidae:
            self.observation_id = None
        if self.observation_id is not None:
            # Ensure upper case
            self.observation_id = str(self.observation_id).upper()

        try:
            self.data_label = ad.data_label()
            if self.data_label is not None:
                # Ensure upper case
                self.data_label = self.data_label.upper()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as dlae:
            if log:
                log.warn("Unable to parse datalabel from datafile: %s" % dlae)
            self.data_label = ""

        self.telescope = gemini_telescope(ad.telescope())
        try:
            self.instrument = gemini_instrument(ad.instrument(), other=True)
        except (TypeError, AttributeError, KeyError, ValueError, IndexError):
            self.instrument = None

        # Date and times part
        try:
            self.ut_datetime = ad.ut_datetime()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ae:
            if log:
                log.warn("Unable to parse UT datetime from datafile: %s" % ae)
            self.ut_datetime = None
        if self.ut_datetime:
            delta = self.ut_datetime - self.UT_DATETIME_SECS_EPOCH
            self.ut_datetime_secs = int(delta.total_seconds())
        try:
            self.local_time = ad.local_time()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as lterr:
            if log:
                log.warn("Unable to find local time in datafile: %s" % lterr)
            self.local_time = None

        # Data Types
        try:
            self.observation_type = gemini_observation_type(ad.observation_type())
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as oterr:
            if log:
                log.warning("Unable to deternime observation type in datafile: %s" % oterr)
            self.observation_type = None

        if 'PINHOLE' in ad.tags:
            self.observation_type = 'PINHOLE'
        if 'RONCHI' in ad.tags:
            self.observation_type = 'RONCHI'

        try:
            self.observation_class = gemini_observation_class(ad.observation_class())
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ocerr:
            if log:
                log.warning("Unable to determine observation class in datafile: %s" % oterr)
            self.observation_class = None
        self.object = ad.object()

        # RA and Dec are not valid for AZEL_TARGET frames
        if 'AZEL_TARGET' not in ad.tags:
            try:
                self.ra = _ra(ad)
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ie:
                if log:
                    log.warn("Unable to read RA from datafile: %s" % ie)
                self.ra = None
            try:
                self.dec = _dec(ad)
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ie:
                if log:
                    log.warn("Unable to read DEC from datafile: %s" % ie)
                self.dec = None
            if type(self.ra) is str:
                self.ra = ratodeg(self.ra)
            if type(self.dec) is str:
                self.dec = dectodeg(self.dec)
            if self.ra is not None and (self.ra > 360.0 or self.ra < 0.0):
                self.ra = None
            if self.dec is not None and (self.dec > 90.0 or self.dec < -90.0):
                self.dec = None

        # These should be in the descriptor function really.
        try:
            azimuth = ad.azimuth()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as azerr:
            if log:
                log.warning("Unable to determine azimuth from datafile: %s" % azerr)
            azimuth = None
        if isinstance(azimuth, str):
            azimuth = dmstodeg(azimuth)
        self.azimuth = azimuth
        try:
            elevation = ad.elevation()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as elerr:
            if log:
                log.warning("Unable to determine elevation from datafile: %s" % elerr)
            elevation = None
        if isinstance(elevation, str):
            elevation = dmstodeg(elevation)
        self.elevation = elevation

        try:
            self.cass_rotator_pa = ad.cass_rotator_pa()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as crperr:
            if log:
                log.warn("Unable to parse cass rotator pa: %s" % crperr)
            self.cass_rotator_pa = None

        try:
            airmass = ad.airmass()
            self.airmass = float(airmass) if isinstance(airmass, str) else airmass
            if self.airmass > 10:
                if self.elevation is not None:
                    try:
                        # use secant(90-elevation) for airmass, converting to radians for numpy
                        cos_value = np.cos(np.radians(90-self.elevation))
                        sec_value = np.arccos(cos_value)
                        if log:
                            log.warning('Bad airmass value, using sec(90-elevation) as an estimate')
                        self.airmass = sec_value
                    except Exception as secamex:
                        if log:
                            log.warning('estimate failed, using None for airmass')
                        self.airmass = None
                else:
                    # invalid airmass, just store None
                    # note also DB can't handle airmass >= 100 (10 cutoff above is per Paul Hirst
                    if log:
                        log.warning('Invalid airmass value and no elevation to estimate from, using None')
                    self.airmass = None
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as airmasserr:
            if log:
                log.warn("Unable to parse airmass: %s" % airmasserr)
            self.airmass = None

        try:
            self.raw_iq = ad.raw_iq()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as rawiqerr:
            if log:
                log.warning("Unable to parse raw_iq: %s" % rawiqerr)
            self.raw_iq=None
        try:
            self.raw_cc = ad.raw_cc()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as rawccerr:
            if log:
                log.warning("Unable to parse raw_cc: %s" % rawccerr)
            self.raw_cc = None
        try:
            self.raw_wv = ad.raw_wv()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as rawwverr:
            if log:
                log.warning("Unable to parse raw_wv: %s" % rawwverr)
            self.raw_wv = None
        try:
            self.raw_bg = ad.raw_bg()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as rawbgerr:
            if log:
                log.warning("Unable to parse raw_bg: %s" % rawbgerr)
            self.raw_bg = None
        try:
            self.requested_iq = ad.requested_iq()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as reqiqerr:
            if log:
                log.warning("Unable to parse requested_iq: %s" % reqiqerr)
            self.requested_iq = None
        try:
            self.requested_cc = ad.requested_cc()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as reqccerr:
            if log:
                log.warning("Unable to parse requested_cc: %s" % reqccerr)
            self.requested_cc = None
        try:
            self.requested_wv = ad.requested_wv()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as reqwverr:
            if log:
                log.warning("Unable to parse requested_wv: %s" % reqwverr)
            self.requested_wv = None
        try:
            self.requested_bg = ad.requested_bg()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as reqbgerr:
            if log:
                log.warning("Unable to parse requested_bg: %s" % reqbgerr)
            self.requested_bg = None

        # Knock illegal characters out of filter names. eg NICI %s.
        # Spaces to underscores.
        try:
            filter_string = ad.filter_name(pretty=True)
            if filter_string:
                self.filter_name = filter_string.replace('%', '').replace(' ', '_')
        except AttributeError:
            self.filter_name = None

        # NICI exposure times are a pain, because there's two of them...
        # Except they're always the same.
        try:
            if self.instrument != 'NICI':
                exposure_time = ad.exposure_time()
            else:
                # NICI exposure_time descriptor is broken
                et_b = ad.phu.get('ITIME_B')
                et_r = ad.phu.get('ITIME_R')
                exposure_time = et_b if et_b else et_r

            # Protect the database from field overflow from junk.
            # The datatype is precision=8, scale=4
            if exposure_time is not None and (exposure_time < 10000 and exposure_time >= 0):
                self.exposure_time = exposure_time
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as et_type_err:
            if log:
                log.warning("Type error parsing exposure time, ignoring")
            

        # Need to remove invalid characters in disperser names, eg gnirs has
        # slashes
        try:
            disperser_string = ad.disperser(pretty=True)
        except (TypeError, AttributeError, KeyError, IndexError) as ae:
            if log:
                log.warn("Unable to read disperser information from datafile: %s" % ae)
            disperser_string = None
        if disperser_string:
            self.disperser = disperser_string.replace('/', '_')

        try:
            self.camera = ad.camera(pretty=True)
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as camerr:
            if log:
                log.warning("Unable to parse camera: %s" % camerr)
            self.camera = None

        if 'SPECT' in ad.tags and 'GPI' not in ad.tags:
            try:
                self.central_wavelength = ad.central_wavelength(asMicrometers=True)
            except (TypeError, AttributeError, KeyError, IndexError):
                self.central_wavelength = None
        try:
            self.wavelength_band = ad.wavelength_band()
        except (TypeError, AttributeError, KeyError, IndexError) as ae:
            if log:
                log.warn("Unable to read disperser information from datafile due to error: %s" % ae)
            self.wavelength_band = None

        try:
            self.focal_plane_mask = ad.focal_plane_mask(pretty=True)
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as  fpmerr:
            if log:
                log.warning("Unable to parse focal plane mask: %s" % fpmerr)
            self.focal_plane_mask = None

        try:
            self.pupil_mask = ad.pupil_mask(pretty=True)
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as pupmerr:
            if log:
                log.warning("Unable to parse pupil_mask: %s" % pupmerr)
            self.pupil_mask = None

        try:
            dvx = ad.detector_x_bin()
        except (TypeError, AttributeError, KeyError, IndexError) as dvxae:
            dvx = None
        try:
            dvy = ad.detector_y_bin()
        except (TypeError, AttributeError, KeyError, IndexError) as dvyae:
            dvy = None
        if (dvx is not None) and (dvy is not None):
            self.detector_binning = "%dx%d" % (dvx, dvy)

        def read_setting(ad, attribute):
            try:
                return str(getattr(ad, attribute)().replace(' ', '_'))
            except (AttributeError, AssertionError):
                return 'None'

        try:
            gainstr = str(ad.gain_setting())
        except (TypeError, AttributeError, KeyError, IndexError) as gsae:
            if log:
                log.warn("Unable to get gain from datafile: %s " % gsae)
            gainstr = ""
        if gainstr in gemini_gain_settings:
            self.detector_gain_setting = gainstr

        try:
            readspeedstr = str(ad.read_speed_setting())
            if readspeedstr in gemini_readspeed_settings:
                self.detector_readspeed_setting = readspeedstr
        except (TypeError, AttributeError, KeyError, IndexError) as ae:
            if log:
                log.warn("Unable to get read speed from datafile: %s " % ae)
            self.detector_readspeed_setting = None

        try:
            welldepthstr = str(ad.well_depth_setting())
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as wdptherr:
            if log:
                log.warning("Unable to parse well_depth_setting: %s" % wdptherr)
            welldepthstr = None
        if welldepthstr in gemini_welldepth_settings:
            self.detector_welldepth_setting = welldepthstr

        if 'GMOS' in ad.tags:
            self.detector_readmode_setting = "NodAndShuffle" \
                if 'NODANDSHUFFLE' in ad.tags else "Classic"
        else:
            try:
                self.detector_readmode_setting = str(ad.read_mode()).replace(' ', '_')
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as rdmderr:
                if log:
                    log.warning("Unable to parse read_mode: %s" % rdmderr)
                self.detector_readmode_setting = None

        try:
            self.detector_roi_setting = ad.detector_roi_setting()
        except (TypeError, AttributeError, KeyError, IndexError) as te:
            if log:
                log.warn("Unable to get ROI setting: %s" % te)
            self.detector_roi_setting = None

        try:
            self.coadds = ad.coadds()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as coaddserr:
            if log:
                log.warning("Unable to parse coadds: %s" % coaddserr)
            self.coadds = None

        # Hack the AO header and LGS for now
        aofold = ad.phu.get('AOFOLD')
        self.adaptive_optics = (aofold == 'IN')

        lgustage = None
        lgsloop = None
        lgustage = ad.phu.get('LGUSTAGE')
        lgsloop = ad.phu.get('LGSLOOP')

        self.laser_guide_star = (lgsloop == 'CLOSED') or (lgustage == 'IN')

        try:
            self.wavefront_sensor = ad.wavefront_sensor()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as wfsenserr:
            if log:
                log.warning("Unable to parse wavefront_seonsor: %s" % wfsenserr)
            self.wavefront_sensor = None

        # And the Spectroscopy and mode items
        self.spectroscopy = False
        self.mode = 'imaging'
        if 'SPECT' in ad.tags:
            self.spectroscopy = True
            self.mode = 'spectroscopy'
            if 'IFU' in ad.tags:
                self.mode = 'IFS'
            if 'MOS' in ad.tags:
                self.mode = 'MOS'
            if 'LS' in ad.tags:
                self.mode = 'LS'
        if 'GPI' in ad.tags and 'POL' in ad.tags:
            self.mode = 'IFP'

        # Set the derived QA state
        # MDF (Mask) files don't have QA state - set to Pass so they show up
        # as expected in search results
        if self.observation_type == 'MASK':
            self.qa_state = 'Pass'
        else:
            try:
                qa_state = ad.qa_state()
                if qa_state in ['Fail', 'CHECK', 'Undefined', 'Usable', 'Pass']:
                    self.qa_state = qa_state
                else:
                    # Default to Undefined. Avoid having NULL values
                    self.qa_state = 'Undefined'
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as qasterr:
                if log:
                    log.warning("Unable to parse qa_state: %s" % qasterr)
                self.qa_state = 'Undefined'

        # Set the release date
        try:
            reldatestring = ad.phu.get('RELEASE')
            if reldatestring:
                reldts = "%s 00:00:00" % reldatestring
                self.release = dateutil.parser.parse(reldts).date()
        except:
            # This exception will trigger if RELEASE date is missing or malformed.
            pass

        # Proprietary coordinates
        self.proprietary_coordinates = False
        if ad.phu.get('PROP_MD') == True:
            self.proprietary_coordinates = True

        # Set the gcal_lamp state
        try:
            gcal_lamp = ad.gcal_lamp()
            if gcal_lamp is not None:
                self.gcal_lamp = gcal_lamp
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as gcallmperr:
            if log:
                log.warning("Unable to parse gcal_lamp: %s" % gcallmperr)
            self.gcal_lamp = None

        # Set the reduction state
        # Note - these are in order - a processed_flat will have
        # both PREPARED and PROCESSED_FLAT in it's types.
        # Here, ensure "highest" value wins.
        tags = ad.tags
        if 'PROCESSED_SCIENCE' in tags:
            self.reduction = 'PROCESSED_SCIENCE'
        elif 'PROCESSED' in tags:
            # Use the image type tag (BIAS, FLAT, ...) to obtain the
            # appropriate reduction status from the lookup table
            kind = list(tags.intersection(list(REDUCTION_STATUS.keys())))
            try:
                self.reduction = REDUCTION_STATUS[kind[0]]
            except (KeyError, IndexError):
                # Supposedly a processed file, but not any that we know of!
                # Mark it as prepared, just in case
                # TODO: Maybe we want to signal an error here?
                self.reduction = 'PROCESSED_UNKNOWN'
        elif 'PREPARED' in tags:
            self.reduction = 'PREPARED'
        else:
            self.reduction = 'RAW'

        try:
            pre_image = ad.phu.get("PREIMAGE")
            if pre_image is not None and (pre_image == "1" or pre_image == "T" or pre_image == True):
                self.pre_image = True
            else:
                self.pre_image = False
        except Exception as ex:
            self.pre_image = False

        # Get the types list
        self.types = str(ad.tags)

        return

    def footprints(self, ad):
        """
        Set footprints based on information in an :class:`astrodata.AstroData` instance.

        This method extracts the WCS from the AstroData instance and uses that to build
        footprint information.

        Parameters
        ----------
        ad : :class:`astrodata.AstroData`
            AstroData object to read footprints from
        """
        retary = {}
        # Horrible hack - GNIRS etc has the WCS for the extension in the PHU!
        if ad.tags.intersection({'GNIRS', 'MICHELLE', 'NIFS'}):
            # If we're not in an RA/Dec TANgent frame, don't even bother
            if (ad.phu.get('CTYPE1') == 'RA---TAN') and (ad.phu.get('CTYPE2') == 'DEC--TAN'):
                hdulist = fits.open(ad.path)
                wcs = pywcs.WCS(hdulist[0].header)
                wcs.array_shape = hdulist[1].data.shape
                try:
                    fp = wcs.calc_footprint()
                    retary['PHU'] = fp
                except SingularMatrixError:
                    # WCS was all zeros.
                    pass
        else:
            # If we're not in an RA/Dec TANgent frame, don't even bother
            # try using fitsio here too
            hdulist = fits.open(ad.path)
            for (hdu, hdr) in zip(hdulist[1:], ad.hdr): # ad.hdr:
                if (hdr.get('CTYPE1') == 'RA---TAN') and (hdr.get('CTYPE2') == 'DEC--TAN'):
                    extension = "%s,%s" % (hdr.get('EXTNAME'), hdr.get('EXTVER'))
                    wcs = pywcs.WCS(hdu.header)
                    if hdu.data is not None and hdu.data.shape:
                        shpe = hdu.data.shape
                        if len(shpe) == 3 and shpe[0] == 1:
                            shpe = shpe[1:]
                            wcs = pywcs.WCS(hdu.header, naxis=2)
                        wcs.array_shape = shpe
                    elif hdulist[1].data is not None and hdulist[1].data.shape:
                        wcs.array_shape = hdulist[1].data.shape
                    try:
                        fp = wcs.calc_footprint()
                        retary[extension] = fp
                    except SingularMatrixError:
                        # WCS was all zeros.
                        pass

        return retary
