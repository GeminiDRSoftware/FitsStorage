from sqlalchemy import Column, ForeignKey, String
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy import Numeric, Boolean, Date
from sqlalchemy import Time, BigInteger, Enum

from sqlalchemy.orm import relation

import dateutil.parser
import datetime
import types

from . import Base
from .diskfile import DiskFile

from ..gemini_metadata_utils import GeminiProgram

from ..gemini_metadata_utils import ratodeg
from ..gemini_metadata_utils import dectodeg
from ..gemini_metadata_utils import dmstodeg
from ..gemini_metadata_utils import gemini_observation_type
from ..gemini_metadata_utils import gemini_telescope
from ..gemini_metadata_utils import gemini_observation_class
from ..gemini_metadata_utils import gemini_instrument
from ..gemini_metadata_utils import gemini_gain_settings
from ..gemini_metadata_utils import gemini_readspeed_settings
from ..gemini_metadata_utils import gemini_welldepth_settings
from ..gemini_metadata_utils import gemini_readmode_settings
from ..gemini_metadata_utils import site_monitor

from astropy import wcs as pywcs
from astropy.wcs import SingularMatrixError
from astropy.io import fits

import astrodata               # For astrodata errors
import gemini_instruments
try:
    import ghost_instruments
except:
    pass

from ..gemini_metadata_utils import obs_types, obs_classes, reduction_states


# ------------------------------------------------------------------------------
# Replace spaces etc in the readmodes with _s
gemini_readmode_settings = [i.replace(' ', '_') for i in gemini_readmode_settings]

# Enumerated Column types
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
}

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
    procsci = Column(String(4))
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
            self.procsci = ad.phu.get('PROCSCI')
        except AttributeError as psciae:
            self.procsci = None

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
        self.observation_type = gemini_observation_type(ad.observation_type())
    
        if 'PINHOLE' in ad.tags:
            self.observation_type = 'PINHOLE'
        if 'RONCHI' in ad.tags:
            self.observation_type = 'RONCHI'
        self.observation_class = gemini_observation_class(ad.observation_class())
        self.object = ad.object()

        # RA and Dec are not valid for AZEL_TARGET frames
        if 'AZEL_TARGET' not in ad.tags:
            try:
                self.ra = ad.ra()
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as ie:
                if log:
                    log.warn("Unable to read RA from datafile: %s" % ie)
                self.ra = None
            try:
                self.dec = ad.dec()
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
        azimuth = ad.azimuth()
        if isinstance(azimuth, str):
            azimuth = dmstodeg(azimuth)
        self.azimuth = azimuth
        elevation = ad.elevation()
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
            self.airmass = ad.airmass()
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as airmasserr:
            if log:
                log.warn("Unable to parse airmass: %s" % airmasserr)
            self.airmass = None
        self.raw_iq = ad.raw_iq()
        self.raw_cc = ad.raw_cc()
        self.raw_wv = ad.raw_wv()
        self.raw_bg = ad.raw_bg()
        self.requested_iq = ad.requested_iq()
        self.requested_cc = ad.requested_cc()
        self.requested_wv = ad.requested_wv()
        self.requested_bg = ad.requested_bg()

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
        if self.instrument != 'NICI':
            exposure_time = ad.exposure_time()
        else:
            # NICI exposure_time descriptor is broken
            et_b = ad.phu.get('ITIME_B')
            et_r = ad.phu.get('ITIME_R')
            exposure_time = et_b if et_b else et_r

        # Protect the database from field overflow from junk.
        # The datatype is precision=8, scale=4
        try:
            if exposure_time is not None and (exposure_time < 10000 and exposure_time >= 0):
                self.exposure_time = exposure_time
        except TypeError as et_type_err:
            if log:
                log.warn("Type rror parsing exposure time, ignoring")
            

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

        self.camera = ad.camera(pretty=True)
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
        self.focal_plane_mask = ad.focal_plane_mask(pretty=True)
        self.pupil_mask = ad.pupil_mask(pretty=True)
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

        welldepthstr = str(ad.well_depth_setting())
        if welldepthstr in gemini_welldepth_settings:
            self.detector_welldepth_setting = welldepthstr

        if 'GMOS' in ad.tags:
            self.detector_readmode_setting = "NodAndShuffle" if ad.tags.intersection({'GMOS', 'NODANDSHUFFLE'}) else "Classic"
        else:
            self.detector_readmode_setting = str(ad.read_mode()).replace(' ', '_')

        try:
            self.detector_roi_setting = ad.detector_roi_setting()
        except (TypeError, AttributeError, KeyError, IndexError) as te:
            if log:
                log.warn("Unable to get ROI setting: %s" % te)
            self.detector_roi_setting = None

        self.coadds = ad.coadds()

        # Hack the AO header and LGS for now
        aofold = ad.phu.get('AOFOLD')
        self.adaptive_optics = (aofold == 'IN')

        lgustage = None
        lgsloop = None
        lgustage = ad.phu.get('LGUSTAGE')
        lgsloop = ad.phu.get('LGSLOOP')

        self.laser_guide_star = (lgsloop == 'CLOSED') or (lgustage == 'IN')
        self.wavefront_sensor = ad.wavefront_sensor()

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
            qa_state = ad.qa_state()
            if qa_state in ['Fail', 'CHECK', 'Undefined', 'Usable', 'Pass']:
                self.qa_state = qa_state
            else:
                # Default to Undefined. Avoid having NULL values
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
        gcal_lamp = ad.gcal_lamp() 
        if gcal_lamp is not None:
            self.gcal_lamp = gcal_lamp

        # Set the reduction state
        # Note - these are in order - a processed_flat will have
        # both PREPARED and PROCESSED_FLAT in it's types.
        # Here, ensure "highest" value wins.
        tags = ad.tags
        if 'PROCESSED' in tags:
            # Use the image type tag (BIAS, FLAT, ...) to obtain the
            # appropriate reduction status from the lookup table
            kind = list(tags.intersection(list(REDUCTION_STATUS.keys())))
            try:
                self.reduction = REDUCTION_STATUS[kind[0]]
            except (KeyError, IndexError):
                # Supposedly a processed file, but not any that we know of!
                # Mark it as prepared, just in case
                # TODO: Maybe we want to signal an error here?
                self.reduction = 'PREPARED'
        elif 'PREPARED' in tags:
            self.reduction = 'PREPARED'
        else:
            self.reduction = 'RAW'

        try:
            pre_image = ad.phu.get("PREIMAGE")
            if pre_image is not None and (pre_image == "1" or pre_image == "T"):
                self.pre_image = True
            else:
                self.pre_image = False
        except Exception as ex:
            self.pre_image = False

        # Get the types list
        self.types = str(ad.tags)

        return


    def footprints(self, ad):
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
