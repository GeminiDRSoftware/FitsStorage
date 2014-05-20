from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy.orm import relation

from orm.header import Header

from astrodata import Errors
from astrodata import AstroData

from . import Base

class Gmos(Base):
    """
    This is the ORM object for the GMOS details.
    This is used for both GMOS-N and GMOS-S
    """
    __tablename__ = 'gmos'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    detector_x_bin = Column(Integer, index=True)
    detector_y_bin = Column(Integer, index=True)
    amp_read_area = Column(Text, index=True)
    read_speed_setting = Column(Text, index=True)
    gain_setting = Column(Text, index=True)
    focal_plane_mask = Column(Text, index=True)
    nodandshuffle = Column(Boolean, index=True)
    nod_count = Column(Integer, index=True)
    nod_pixels = Column(Integer, index=True)
    prepared = Column(Boolean, index=True)
    overscan_subtracted = Column(Boolean, index=True)
    overscan_trimmed = Column(Boolean, index=True)

    def __init__(self, header):
        self.header = header

        # Populate from the astrodata object
        self.populate()

    def populate(self):
        # Get an AstroData object on it
        if(self.header.diskfile.uncompressed_cache_file):
            fullpath = self.header.diskfile.uncompressed_cache_file
        else:
            fullpath = self.header.diskfile.fullpath()

        try:
            ad = AstroData(fullpath, mode="readonly")
            # Populate values
            self.disperser = ad.disperser().for_db()
            self.filter_name = ad.filter_name().for_db()
            self.detector_x_bin = ad.detector_x_bin().for_db()
            self.detector_y_bin = ad.detector_y_bin().for_db()
            self.amp_read_area = ad.amp_read_area().for_db()
            self.read_speed_setting = ad.read_speed_setting().for_db()
            self.gain_setting = ad.gain_setting().for_db()
            self.focal_plane_mask = ad.focal_plane_mask().for_db()
            self.nodandshuffle = ad.is_type('GMOS_NODANDSHUFFLE')
            if(self.nodandshuffle):
                self.nod_count = ad.nod_count().for_db()
                self.nod_pixels = ad.nod_pixels().for_db()
            self.prepared = ad.is_type('PREPARED')
            self.overscan_trimmed = ad.is_type('OVERSCAN_TRIMMED')
            self.overscan_subtracted = ad.is_type('OVERSCAN_SUBTRACTED')

            ad.close()
        except Errors.CorruptDataError:
            # Astrodata open failed or there was some other exception that we want to ignore
            ad.close()
            pass
        except:
            # Astrodata open failed or there was some other exception that we want to report
            ad.close()
            raise


