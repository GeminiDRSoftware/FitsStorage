from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation

from . import Base
from .header import Header

# Enumerated Column types
READ_MODES = ['Very Faint Objects', 'Faint Objects', 'Bright Objects', 'Very Bright Objects', 'Invalid']
READ_MODE_ENUM = Enum(*READ_MODES, name='gnirs_read_mode')

WELL_DEPTH_SETTINGS = ['Shallow', 'Deep', 'Invalid']
WELL_DEPTH_SETTING_ENUM = Enum(*WELL_DEPTH_SETTINGS, name='gnirs_well_depth_setting')


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
    read_mode = Column(READ_MODE_ENUM, index=True)
    well_depth_setting = Column(WELL_DEPTH_SETTING_ENUM, index=True)
    camera = Column(Text, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()

        read_mode = ad.read_mode()
        if read_mode in READ_MODES:
            self.read_mode = read_mode

        well_depth_setting = ad.well_depth_setting()
        if well_depth_setting in WELL_DEPTH_SETTINGS:
            self.well_depth_setting = well_depth_setting

        self.camera = ad.camera()
        self.focal_plane_mask = ad.focal_plane_mask()
