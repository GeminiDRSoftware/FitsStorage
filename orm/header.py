from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Numeric, Boolean, Date, Time
from sqlalchemy.orm import relation

import dateutil.parser

from . import Base
from orm.diskfile import DiskFile

from astrodata import AstroData
import pywcs

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
    observation_id = Column(Text, index=True)
    data_label = Column(Text, index=True)
    telescope = Column(Text, index=True)
    instrument = Column(Text, index=True)
    ut_datetime = Column(DateTime(timezone=False), index=True)
    local_time = Column(Time(timezone=False))
    observation_type = Column(Text, index=True)
    observation_class = Column(Text, index=True)
    object = Column(Text)
    ra = Column(Numeric(precision=16, scale=12))
    dec = Column(Numeric(precision=16, scale=12))
    azimuth = Column(Numeric(precision=16, scale=12))
    elevation = Column(Numeric(precision=16, scale=12))
    cass_rotator_pa = Column(Numeric(precision=16, scale=12))
    airmass = Column(Numeric(precision=8, scale=6))
    filter_name = Column(Text)
    exposure_time = Column(Numeric(precision=8, scale=4))
    disperser = Column(Text)
    central_wavelength = Column(Numeric(precision=8, scale=6))
    wavelength_band = Column(Text)
    focal_plane_mask = Column(Text)
    detector_binning = Column(Text)
    detector_config = Column(Text)
    detector_roi_setting = Column(Text)
    spectroscopy = Column(Boolean, index=True)
    mode = Column(Text, index=True)
    adaptive_optics = Column(Boolean)
    laser_guide_star = Column(Boolean)
    wavefront_sensor = Column(Text)
    raw_iq = Column(Integer)
    raw_cc = Column(Integer)
    raw_wv = Column(Integer)
    raw_bg = Column(Integer)
    requested_iq = Column(Integer)
    requested_cc = Column(Integer)
    requested_wv = Column(Integer)
    requested_bg = Column(Integer)
    qa_state = Column(Text, index=True)
    release = Column(Date)
    reduction = Column(Text, index=True)
    types = Column(Text)
    phot_standard = Column(Boolean)

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id
        self.populate_fits(diskfile)

    def __repr__(self):
        return "<Header('%s', '%s')>" % (self.id, self.diskfile_id)

    def populate_fits(self, diskfile):
        """
        Populates header table values from the FITS headers of the file.
        Uses the AstroData object to access the file.
        """

        # The header object is unusual in that we directly pass the constructor a diskfile
        # object which may have an ad_object in it.
        if(diskfile.ad_object is not None):
            ad = diskfile.ad_object
            local_ad = False
        else:
            if(diskfile.uncompressed_cache_file):
                fullpath = diskfile.uncompressed_cache_file
            else:
                fullpath = diskfile.fullpath()
            ad = AstroData(fullpath, mode='readonly')
            local_ad = True
    
        try:
            # Basic data identification part
            self.program_id = ad.program_id().for_db()
            if(self.program_id is not None):
                self.engineering = ('ENG' in self.program_id)
                self.science_verification = ('SV' in self.program_id)
            self.observation_id = ad.observation_id().for_db()
            self.data_label = ad.data_label().for_db()
            self.telescope = ad.telescope().for_db()
            self.instrument = ad.instrument().for_db()

            # Date and times part
            self.ut_datetime = ad.ut_datetime().for_db()
            self.local_time = ad.local_time().for_db()

            # Data Types
            self.observation_type = ad.observation_type().for_db()
            if('GNIRS_PINHOLE' in ad.types):
                self.observation_type = 'PINHOLE'
            if('NIFS_RONCHI' in ad.types):
                self.observation_type = 'RONCHI'
            self.observation_class = ad.observation_class().for_db()
            self.object = ad.object().for_db()
            self.ra = ad.ra().for_db()
            self.dec = ad.dec().for_db()
            self.azimuth = ad.azimuth().for_db()
            self.elevation = ad.elevation().for_db()
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

            self.filter_name = ad.filter_name(pretty=True).for_db()
            # NICI exposure times are a pain, because there's two of them... Not sure how to handle this for now.
            if(self.instrument != 'NICI'):
                self.exposure_time = ad.exposure_time().for_db()
            self.disperser = ad.disperser(pretty=True).for_db()
            if('SPECT' in ad.types):
                self.central_wavelength = ad.central_wavelength(asMicrometers=True).for_db()
            self.wavelength_band = ad.wavelength_band().for_db()
            self.focal_plane_mask = ad.focal_plane_mask(pretty=True).for_db()
            dvx = ad.detector_x_bin()
            dvy = ad.detector_y_bin()
            if((not dvx.is_none()) and (not dvy.is_none())):
                self.detector_binning = "%dx%d" % (int(ad.detector_x_bin()), int(ad.detector_y_bin()))

            try:
                gainsetting = str(ad.gain_setting())
            except ():
                gainsetting = "None"
            try:
                readspeedsetting = str(ad.read_speed_setting())
            except ():
                readspeedsetting = "None"
            nodandshuffle = "NodAndShuffle" if ad.is_type("GMOS_NODANDSHUFFLE") else ""
            self.detector_config = "%s %s %s" % (gainsetting, readspeedsetting, nodandshuffle)

            self.detector_roi_setting = ad.detector_roi_setting().for_db()


            # Hack the AO header and LGS for now
            try:
                aofold = ad.phu_get_key_value('AOFOLD')
                self.adaptive_optics = (aofold == 'IN')
            except ():
                pass

            try:
                lgustage = ad.phu_get_key_value('LGUSTAGE')
                self.laser_guide_star = (lgustage == 'IN')
            except ():
                pass

            try:
                lgsloop = ad.phu_get_key_value('LGSLOOP')
                self.laser_guide_star = (lgsloop == 'CLOSED')
            except ():
                pass


            self.wavefront_sensor = ad.wavefront_sensor().for_db()

            # And the Spectroscopy and mode items
            self.spectroscopy = False
            if('SPECT' in ad.types):
                self.spectroscopy = True
                self.mode = 'spectroscopy'
                if('IFU' in ad.types):
                    self.mode = 'IFU'
                if('MOS' in ad.types):
                    self.mode = 'MOS'
                if('LS' in ad.types):
                    self.mode = 'LS'
            else:
                self.spectroscopy = False
                self.mode = 'imaging'

            # Set the derived QA state and release date
            self.qa_state = ad.qa_state().for_db()
            try:
                reldatestring = ad.phu_get_key_value('RELEASE')
                if(reldatestring):
                    reldts = "%s 00:00:00" % reldatestring
                    self.release = dateutil.parser.parse(reldts).date()
            except:
                # This exception will trigger if RELEASE date is missing or malformed.
                pass


            # Set the reduction state
            self.reduction = 'RAW'
            if('PREPARED' in ad.types):
                self.reduction = 'PREPARED'
            if('PROCESSED_FLAT' in ad.types):
                self.reduction = 'PROCESSED_FLAT'
            if('PROCESSED_BIAS' in ad.types):
                self.reduction = 'PROCESSED_BIAS'
            if('PROCESSED_FRINGE' in ad.types):
                self.reduction = 'PROCESSED_FRINGE'
            if('PROCESSED_DARK' in ad.types):
                self.reduction = 'PROCESSED_DARK'
            if('PROCESSED_ARC' in ad.types):
                self.reduction = 'PROCESSED_ARC'

            # Get the types list
            self.types = str(ad.types)

        except:
            # Something failed accessing the astrodata 
            raise

        finally:
            if(local_ad):
                ad.close()

    def footprints(self, ad):
        retary = {}
        # Horrible hack - GNIRS etc has the WCS in the PHU
        if(('GNIRS' in ad.types) or ('MICHELLE' in ad.types) or ('NIFS' in ad.types)):
            # If we're not in an RA/Dec TANgent frame, don't even bother
            if((ad.phu_get_key_value('CTYPE1') == 'RA---TAN') and (ad.phu_get_key_value('CTYPE2') == 'DEC--TAN')):
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
                if((ad[i].get_key_value('CTYPE1') == 'RA---TAN') and (ad[i].get_key_value('CTYPE2') == 'DEC--TAN')):
                    extension = "%s,%s" % (ad[i].extname(), ad[i].extver())
                    wcs = pywcs.WCS(ad[i].header)
                    try:
                        fp = wcs.calcFootprint()
                        retary[extension] = fp
                    except pywcs._pywcs.SingularMatrixError:
                        # WCS was all zeros.
                        pass

        return retary
