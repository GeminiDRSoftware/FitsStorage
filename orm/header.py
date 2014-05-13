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
        if(diskfile.uncompressed_cache_file):
            fullpath = diskfile.uncompressed_cache_file
        else:
            fullpath = diskfile.fullpath()
    
        # Try and open it as a fits file
        ad = None
        try:
            ad = AstroData(fullpath, mode='readonly')

            # Basic data identification part
            try:
                self.program_id = ad.program_id().for_db()
            except ():
                pass
            try:
                self.observation_id = ad.observation_id().for_db()
            except ():
                pass
            try:
                self.data_label = ad.data_label().for_db()
            except ():
                pass
            try:
                self.telescope = ad.telescope().for_db()
            except ():
                pass
            try:
                self.instrument = ad.instrument().for_db()
            except ():
                pass

            # Date and times part
            try:
                self.ut_datetime = ad.ut_datetime().for_db()
            except ():
                pass

            try:
                self.local_time = ad.local_time().for_db()
            except ():
                pass

            # Data Types
            try:
                self.observation_type = ad.observation_type().for_db()
                if('GNIRS_PINHOLE' in ad.types):
                    self.observation_type = 'PINHOLE'
                if('NIFS_RONCHI' in ad.types):
                    self.observation_type = 'RONCHI'
            except ():
                pass
            try:
                self.observation_class = ad.observation_class().for_db()
            except ():
                pass
            try:
                self.object = ad.object().for_db()
            except ():
                pass
            try:
                self.ra = ad.ra().for_db()
            except ():
                pass
            try:
                self.dec = ad.dec().for_db()
            except ():
                pass
            try:
                self.azimuth = ad.azimuth().for_db()
            except ():
                pass
            try:
                self.elevation = ad.elevation().for_db()
            except ():
                pass
            try:
                self.cass_rotator_pa = ad.cass_rotator_pa().for_db()
            except ():
                pass
            try:
                self.airmass = ad.airmass().for_db()
            except ():
                pass
            try:
                self.raw_iq = ad.raw_iq().for_db()
            except ():
                pass
            try:
                self.raw_cc = ad.raw_cc().for_db()
            except ():
                pass
            try:
                self.raw_wv = ad.raw_wv().for_db()
            except ():
                pass
            try:
                self.raw_bg = ad.raw_bg().for_db()
            except ():
                pass
            try:
                self.requested_iq = ad.requested_iq().for_db()
            except ():
                pass
            try:
                self.requested_cc = ad.requested_cc().for_db()
            except ():
                pass
            try:
                self.requested_wv = ad.requested_wv().for_db()
            except ():
                pass
            try:
                self.requested_bg = ad.requested_bg().for_db()
            except ():
                pass

            try:
                self.filter_name = ad.filter_name(pretty=True).for_db()
            except ():
                pass
            try:
                # NICI exposure times are a pain, because there's two of them... Not sure how to handle this for now.
                if(self.instrument != 'NICI'):
                    self.exposure_time = ad.exposure_time().for_db()
            except ():
                pass
            try:
                self.disperser = ad.disperser(pretty=True).for_db()
            except ():
                pass
            if('SPECT' in ad.types):
                try:
                    self.central_wavelength = ad.central_wavelength(asMicrometers=True).for_db()
                except ():
                    pass
            try:
                self.wavelength_band = ad.wavelength_band().for_db()
            except ():
                pass
            try:
                self.focal_plane_mask = ad.focal_plane_mask(pretty=True).for_db()
            except ():
                pass
            try:
                dvx = ad.detector_x_bin()
                dvy = ad.detector_y_bin()
                if((not dvx.is_none()) and (not dvy.is_none())):
                    self.detector_binning = "%dx%d" % (int(ad.detector_x_bin()), int(ad.detector_y_bin()))
            except ():
                pass

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

            try:
                self.detector_roi_setting = ad.detector_roi_setting().for_db()
            except ():
                pass


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


            try:
                self.wavefront_sensor = ad.wavefront_sensor().for_db()
            except ():
                pass

            # And the Spectroscopy header
            self.spectroscopy = False
            if('SPECT' in ad.types):
                self.spectroscopy = True

            # Set the derived QA state and release date
            try:
                self.qa_state = ad.qa_state().for_db()
            except ():
                pass
            try:
                reldatestring = ad.phu_get_key_value('RELEASE')
                if(reldatestring):
                    reldts = "%s 00:00:00" % reldatestring
                    self.release = dateutil.parser.parse(reldts).date()
            except ():
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

            if(ad is not None):
                ad.close()
        except:
            # Astrodata open failed or there was some other exception
            if(ad is not None):
                ad.close()
            raise

    def footprints(self):
        if(diskfile.uncompressed_cache_file):
            fullpath = uncompressed_cache_file
        else:
            fullpath = self.diskfile.fullpath()

        # Try and open it as a fits file
        ad = 0
        retary = {}
        try:
            ad = AstroData(fullpath, mode='readonly')
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


            ad.close()
            return retary

        except:
            # Astrodata open failed or there was some other exception
            ad.close()
            raise


