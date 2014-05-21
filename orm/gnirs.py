from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from orm.header import Header

from astrodata import AstroData

from . import Base

class Gnirs(Base):
    """
    This is the ORM object for the GNIRS details
    """
    __tablename__ = 'gnirs'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(Text, index=True)
    well_depth_setting = Column(Text, index=True)
    coadds = Column(Integer, index=True)
    camera = Column(Text, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.disperser = ad.disperser().for_db()
        self.filter_name = ad.filter_name().for_db()
        self.read_mode = ad.read_mode().for_db()
        self.well_depth_setting = ad.well_depth_setting().for_db()
        self.coadds = ad.coadds().for_db()
        self.camera = ad.camera().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
