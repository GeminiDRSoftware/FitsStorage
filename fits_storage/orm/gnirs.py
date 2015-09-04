from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation
from sqlalchemy.exc import DataError

from . import Base
from .header import Header

# Enumerated Column types
GNIRS_READ_MODE_ENUM = Enum('Very Faint Objects', 'Faint Objects', 'Bright Objects', 'Very Bright Objects', 'Invalid', name='gnirs_read_mode')
GNIRS_WELL_DEPTH_SETTING_ENUM = Enum('Shallow', 'Deep', 'Invalid', name='gnirs_well_depth_setting')


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
    read_mode = Column(GNIRS_READ_MODE_ENUM, index=True)
    well_depth_setting = Column(GNIRS_WELL_DEPTH_SETTING_ENUM, index=True)
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
        self.camera = ad.camera().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
