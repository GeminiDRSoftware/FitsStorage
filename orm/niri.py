from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from orm.header import Header

from astrodata import AstroData

from . import Base

class Niri(Base):
    """
    This is the ORM object for the NIRI details
    """
    __tablename__ = 'niri'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(Text, index=True)
    well_depth_setting = Column(Text, index=True)
    data_section = Column(Text, index=True)
    coadds = Column(Integer, index=True)
    camera = Column(Text, index=True)
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
            try:
                self.disperser = ad.disperser().for_db()
            except ():
                pass
            try:
                self.filter_name = ad.filter_name().for_db()
            except ():
                pass
            try:
                self.read_mode = ad.read_mode().for_db()
            except ():
                pass
            try:
                self.well_depth_setting = ad.well_depth_setting().for_db()
            except ():
                pass
            try:
                # the str() is a temp workaround 20110404 PH
                self.data_section = str(ad.data_section().for_db())
            except ():
                pass
            try:
                self.coadds = ad.coadds().for_db()
            except ():
                pass
            try:
                self.camera = ad.camera().for_db()
            except ():
                pass
            try:
                self.focal_plane_mask = ad.focal_plane_mask().for_db()
            except ():
                pass
            ad.close()
        except:
            # Astrodata open failed or there was some other exception
            ad.close()
            raise


