from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from orm.header import Header

from astrodata import AstroData

from . import Base

class Michelle(Base):
    """
    This is the ORM object for the MICHELLE details
    """
    __tablename__ = 'michelle'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(Text, index=True)
    coadds = Column(Integer, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header):
        self.header = header

        # Populate from an astrodata object
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
            self.read_mode = ad.read_mode().for_db()
            self.coadds = ad.coadds().for_db()
            self.focal_plane_mask = ad.focal_plane_mask().for_db()
            ad.close()
        except:
            # Astrodata open failed or there was some other exception
            ad.close()
            raise

