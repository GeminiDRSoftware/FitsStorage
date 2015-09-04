from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation


from .header import Header

from . import Base

# Enumerated Column types
NIRI_READ_MODE_ENUM = Enum('High Background', 'Medium Background', 'Low Background', 'Invalid', name='niri_read_mode')
NIRI_WELL_DEPTH_SETTING_ENUM = Enum('Shallow', 'Deep', 'Invalid', name='niri_well_depth_setting')

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
    read_mode = Column(NIRI_READ_MODE_ENUM, index=True)
    well_depth_setting = Column(NIRI_WELL_DEPTH_SETTING_ENUM, index=True)
    data_section = Column(Text, index=True)
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
        # the str() is a temp workaround 20110404 PH
        self.data_section = str(ad.data_section().for_db())
        self.camera = ad.camera().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
