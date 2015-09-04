from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Numeric, Boolean, Date, Time, BigInteger, Enum
from sqlalchemy.orm import relation

import dateutil.parser
import datetime
import types

from . import Base
from .diskfile import DiskFile
from ..gemini_metadata_utils import ratodeg, dectodeg, dmstodeg, gemini_observation_type, gemini_telescope, gemini_observation_class, gemini_instrument
from ..gemini_metadata_utils import GeminiProgram

from astrodata import AstroData
import pywcs

import astrodata # For astrodata.Errors

from ..gemini_metadata_utils import obs_types, obs_classes, reduction_states

# Enumerated Column types
OBSTYPE_ENUM = Enum(*obs_types, name='obstype')
OBSCLASS_ENUM = Enum(*obs_classes, name='obsclass')
REDUCTION_STATE_ENUM = Enum(*reduction_states, name='reduction_state')
TELESCOPE_ENUM = Enum('Gemini-North', 'Gemini-South', name='telescope')
QASTATE_ENUM = Enum('Fail', 'CHECK', 'Undefined', 'Usable', 'Pass', name='qa_state')
MODE_ENUM = Enum('imaging', 'spectroscopy', 'LS', 'MOS', 'IFS', 'IFP', name='mode')

class Header(Base):
    """
    This is the ORM class for the Header table
    """
    __tablename__ = 'header'

    
    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile = relation(DiskFile, order_by=id)
    program_id = Column(Text, index=True)
    engineering = Column(Boolean, index=True)
    science_verification = Column(Boolean, index=True)
    calibration_program = Column(Boolean, index=True)
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
    # NOTE - TEMPORARILY GET ALL RA/DEC VALUES TO ASSIST TROUBLESHOOTING
    # WILL WANT TO GET RID OF ALL BUT ra,dec 
    ra = Column(Numeric(precision=16, scale=12), index=True)
    dec = Column(Numeric(precision=16, scale=12), index=True)
    wcs_ra = Column(Numeric(precision=16, scale=12), index=True)
    wcs_dec = Column(Numeric(precision=16, scale=12), index=True)
    target_ra = Column(Numeric(precision=16, scale=12), index=True)
    target_dec = Column(Numeric(precision=16, scale=12), index=True)
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
    detector_binning = Column(Text)
    detector_config = Column(Text)
    detector_roi_setting = Column(Text)
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
    types = Column(Text)
    phot_standard = Column(Boolean)
    proprietary_coordinates = Column(Boolean)

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id
        self.populate_fits(diskfile)

    def __repr__(self):
        return "<Header('%s', '%s')>" % (self.id, self.diskfile_id)

    UT_DATETIME_SECS_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0)
    def populate_fits(self, diskfile):
        """
        Populates header table values from the FITS headers of the file.
        Uses the AstroData object to access the file.
        """

        # The header object is unusual in that we directly pass the constructor a diskfile
        # object which may have an ad_object in it.
        if diskfile.ad_object is not None:
            ad = diskfile.ad_object
            local_ad = False
        else:
            if diskfile.uncompressed_cache_file:
                fullpath = diskfile.uncompressed_cache_file
            else:
                fullpath = diskfile.fullpath()
            ad = AstroData(fullpath, mode='readonly')
            local_ad = True

        try:
            # Basic data identification section
            # Parse Program ID
            self.program_id = ad.program_id().for_db()
            if self.program_id is not None:
                # Ensure upper case
                self.program_id = self.program_id.upper()
                # Set eng and sv booleans
                gemprog = GeminiProgram(self.program_id)
                self.engineering = gemprog.is_eng or not gemprog.valid
                self.science_verification = gemprog.is_sv
                self.calibration_program = gemprog.is_cal
            else:
                # program ID is None - mark as engineering
                self.engineering = True

            self.observation_id = ad.observation_id().for_db()
            if self.observation_id is not None:
                # Ensure upper case
                self.observation_id = self.observation_id.upper()

            self.data_label = ad.data_label().for_db()
            if self.data_label is not None:
                # Ensure upper case
                self.data_label = self.data_label.upper()

            self.telescope = gemini_telescope(ad.telescope().for_db())
            self.instrument = gemini_instrument(ad.instrument().for_db(), other=True)

            # Date and times part
            self.ut_datetime = ad.ut_datetime().for_db()
            if self.ut_datetime:
                delta = self.ut_datetime - self.UT_DATETIME_SECS_EPOCH
                self.ut_datetime_secs = int(delta.total_seconds())
            self.local_time = ad.local_time().for_db()

            # Data Types
            self.observation_type = gemini_observation_type(ad.observation_type().for_db())

            if 'GNIRS_PINHOLE' in ad.types:
                self.observation_type = 'PINHOLE'
            if 'NIFS_RONCHI' in ad.types:
                self.observation_type = 'RONCHI'
            self.observation_class = gemini_observation_class(ad.observation_class().for_db())
            self.object = ad.object().for_db()

            # RA and Dec are not valid for AZEL_TARGET frames
            if 'AZEL_TARGET' not in ad.types:
                self.ra = ad.ra().for_db()
                self.dec = ad.dec().for_db()
                self.wcs_ra = ad.ra().for_db()
                self.wcs_dec = ad.dec().for_db()
                self.target_ra = ad.target_ra(offset=True, icrs=True).for_db()
                self.target_dec = ad.target_dec(offset=True, icrs=True).for_db()
                if type(self.ra) is str:
                    self.ra = ratodeg(self.ra)
                if type(self.dec) is str:
                    self.dec = dectodeg(self.dec)
                if self.ra > 360.0 or self.ra < 0.0:
                    self.ra = None
                if self.dec > 90.0 or self.dec < -90.0:
                    self.dec = None
                if self.target_ra > 360.0 or self.target_ra < 0.0:
                    self.target_ra = None
                if self.target_dec > 90.0 or self.target_dec < -90.0:
                    self.target_dec = None
                if self.wcs_ra > 360.0 or self.wcs_ra < 0.0:
                    self.wcs_ra = None
                if self.wcs_dec > 90.0 or self.wcs_dec < -90.0:
                    self.wcs_dec = None

            # These should be in the descriptor function really.
            azimuth = ad.azimuth().for_db()
            if type(azimuth) is types.StringType:
                azimuth = dmstodeg(azimuth)
            self.azimuth = azimuth
            elevation = ad.elevation().for_db()
            if type(elevation) is types.StringType:
                elevation = dmstodeg(elevation)
            self.elevation = elevation

            self.cass_rotator_pa = ad.cass_rotator_pa().for_db()
            self.airmass = ad.airmass().for_db()
            self.raw_iq = ad.raw_iq().for_db()
            self.raw_cc = ad.raw_cc().for_db()
            self.raw_wv = ad.raw_wv().for_db()
            self.raw_bg = ad.raw_bg().for_db()
            self.requested_iq = ad.requested_iq().for_db()
            self.requested_cc = ad.requested_cc().for_db()
            self.requested_wv = ad.requested_wv().for_db()
            self.requested_bg = ad.requested_bg().for_db()

            # Knock illegal characters out of filter names. eg NICI %s. Spaces to underscores.
            filter_string = ad.filter_name(pretty=True).for_db()
            if filter_string:
                self.filter_name = filter_string.replace('%', '').replace(' ', '_')

            # NICI exposure times are a pain, because there's two of them... Except they're always the same
            if self.instrument != 'NICI':
                exposure_time = ad.exposure_time().for_db()
            else:
                # NICI exposure_time descriptor is broken
                et_b = ad.phu_get_key_value('ITIME_B')
                et_r = ad.phu_get_key_value('ITIME_R')
                exposure_time = et_b if et_b else et_r

            # Protect the database from field overflow from junk.
            # The datatype is precision=8, scale=4
            if exposure_time < 10000 and exposure_time >= 0:
                self.exposure_time = exposure_time
            

            # Need to remove invalid characters in disperser names, eg gnirs has slashes
            disperser_string = ad.disperser(pretty=True).for_db()
            if disperser_string:
                self.disperser = disperser_string.replace('/', '_')

            self.camera = ad.camera(pretty=True).for_db()
            if 'SPECT' in ad.types and 'GPI' not in ad.types:
                self.central_wavelength = ad.central_wavelength(asMicrometers=True).for_db()
            self.wavelength_band = ad.wavelength_band().for_db()
            self.focal_plane_mask = ad.focal_plane_mask(pretty=True).for_db()
            dvx = ad.detector_x_bin()
            dvy = ad.detector_y_bin()
            if (not dvx.is_none()) and (not dvy.is_none()):
                self.detector_binning = "%dx%d" % (int(ad.detector_x_bin()), int(ad.detector_y_bin()))

            gainsetting = str(ad.gain_setting()).replace(' ', '_')
            readspeedsetting = str(ad.read_speed_setting()).replace(' ', '_')
            welldepthsetting = str(ad.well_depth_setting()).replace(' ', '_')
            readmode = str(ad.read_mode()).replace(' ', '_')
            nodandshuffle = "NodAndShuffle" if "GMOS_NODANDSHUFFLE" in ad.types else ""
            self.detector_config = ' '.join([gainsetting, readspeedsetting, nodandshuffle, welldepthsetting, readmode])

            self.detector_roi_setting = ad.detector_roi_setting().for_db()

            self.coadds = ad.coadds().for_db()

            # Hack the AO header and LGS for now
            try:
                aofold = ad.phu_get_key_value('AOFOLD')
                self.adaptive_optics = (aofold == 'IN')
            except ():
                pass

            lgustage = None
            lgsloop = None
            try:
                lgustage = ad.phu_get_key_value('LGUSTAGE')
                lgsloop = ad.phu_get_key_value('LGSLOOP')
            except:
                pass

            self.laser_guide_star = (lgsloop == 'CLOSED') or (lgustage == 'IN')


            self.wavefront_sensor = ad.wavefront_sensor().for_db()

            # And the Spectroscopy and mode items
            self.spectroscopy = False
            self.mode = 'imaging'
            if 'SPECT' in ad.types:
                self.spectroscopy = True
                self.mode = 'spectroscopy'
                if 'IFU' in ad.types:
                    self.mode = 'IFS'
                if 'MOS' in ad.types:
                    self.mode = 'MOS'
                if 'LS' in ad.types:
                    self.mode = 'LS'
            if 'GPI' in ad.types and 'POL' in ad.types:
                self.mode = 'IFP'

            # Set the derived QA state
            # MDF (Mask) files don't have QA state - set to Pass so they show up as expected in search results
            if self.observation_type == 'MASK':
                self.qa_state = 'Pass'
            else:
                qa_state = ad.qa_state().for_db()
                if qa_state in ['Fail', 'CHECK', 'Undefined', 'Usable', 'Pass']:
                    self.qa_state = qa_state
                else:
                    # Default to Undefined. Avoid having NULL values
                    self.qa_state = 'Undefined'

            # Set the release date
            try:
                reldatestring = ad.phu_get_key_value('RELEASE')
                if reldatestring:
                    reldts = "%s 00:00:00" % reldatestring
                    self.release = dateutil.parser.parse(reldts).date()
            except:
                # This exception will trigger if RELEASE date is missing or malformed.
                pass

            # Proprietary coordinates
            self.proprietary_coordinates = False
            if ad.phu_get_key_value('PROP_MD') == True:
                self.proprietary_coordinates = True

            # Set the gcal_lamp state
            gcal_lamp = ad.gcal_lamp().for_db() 
            if gcal_lamp != 'None':
                self.gcal_lamp = gcal_lamp


            # Set the reduction state
            self.reduction = 'RAW'

            # Note - these are in order - a processed_flat will have
            # both PREPARED and PROCESSED_FLAT in it's types.
            # Here, ensure "highest" value wins.
            if 'PREPARED' in ad.types:
                self.reduction = 'PREPARED'
            if 'PROCESSED_FLAT' in ad.types:
                self.reduction = 'PROCESSED_FLAT'
            if 'PROCESSED_BIAS' in ad.types:
                self.reduction = 'PROCESSED_BIAS'
            if 'PROCESSED_FRINGE' in ad.types:
                self.reduction = 'PROCESSED_FRINGE'
            if 'PROCESSED_DARK' in ad.types:
                self.reduction = 'PROCESSED_DARK'
            if 'PROCESSED_ARC' in ad.types:
                self.reduction = 'PROCESSED_ARC'

            # Get the types list
            self.types = str(ad.types)

        except astrodata.Errors.DescriptorInfrastructureError:
            # This happens anytime an eng file does not get identified as gemini data
            pass

        except:
            # Something failed accessing the astrodata
            raise

        finally:
            if local_ad:
                ad.close()

    def footprints(self, ad):
        retary = {}
        # Horrible hack - GNIRS etc has the WCS for the extension in the PHU!
        if ('GNIRS' in ad.types) or ('MICHELLE' in ad.types) or ('NIFS' in ad.types):
            # If we're not in an RA/Dec TANgent frame, don't even bother
            if (ad.phu_get_key_value('CTYPE1') == 'RA---TAN') and (ad.phu_get_key_value('CTYPE2') == 'DEC--TAN'):
                wcs = pywcs.WCS(ad.phu.header)
                try:
                    fp = wcs.calcFootprint()
                    retary['PHU'] = fp
                except pywcs._pywcs.SingularMatrixError:
                    # WCS was all zeros.
                    pass
        else:
            # If we're not in an RA/Dec TANgent frame, don't even bother
            for i in range(len(ad)):
                if (ad[i].get_key_value('CTYPE1') == 'RA---TAN') and (ad[i].get_key_value('CTYPE2') == 'DEC--TAN'):
                    extension = "%s,%s" % (ad[i].extname(), ad[i].extver())
                    wcs = pywcs.WCS(ad[i].header)
                    try:
                        fp = wcs.calcFootprint()
                        retary[extension] = fp
                    except pywcs._pywcs.SingularMatrixError:
                        # WCS was all zeros.
                        pass

        return retary
