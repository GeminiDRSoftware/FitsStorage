from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation

from .header import Header

from . import Base

# Enumerated Column types
READ_MODES = ['High Background', 'Medium Background', 'Low Background', 'Invalid']
READ_MODE_ENUM = Enum(*READ_MODES, name='niri_read_mode')

WELL_DEPTHS = ['Shallow', 'Deep', 'Invalid']
WELL_DEPTH_SETTING_ENUM = Enum(*WELL_DEPTHS, name='niri_well_depth_setting')

# Spectroscopy is decommissioned, so go ahead and enumerate the dispersers
DISPERSERS = ['Hgrismf32_G5228', 'Hgrism_G5203', 'Jgrismf32_G5226', 'Jgrism_G5202', 'Kgrismf32_G5227', 
              'Kgrism_G5204', 'Lgrism_G5205', 'Mgrism_G5206', 'MIRROR']
DISPERSER_ENUM = Enum(*DISPERSERS, name='niri_disperser')

# Very unlikely to get get new ones of these:
CAMERAS = ['datum', 'f13.9', 'f14', 'f32', 'f6', 'INVALID', 'No Value', 'UNKNOWN']
CAMERA_ENUM = Enum(*CAMERAS, name='niri_camera')

DATA_SECTIONS = ['[0, 1024, 0, 1024]', '[0, 256, 0, 256]', '[0, 512, 0, 512]', '[0, 768, 0, 768]', 
                 '[256, 769, 0, 1024]', '[256, 769, 256, 768]', '[64, 192, 64, 192]', 'None']
DATA_SECTION_ENUM = Enum(*DATA_SECTIONS, name='niri_data_section')



class Niri(Base):
    """
    This is the ORM object for the NIRI details
    """
    __tablename__ = 'niri'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(DISPERSER_ENUM, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(READ_MODE_ENUM, index=True)
    well_depth_setting = Column(WELL_DEPTH_SETTING_ENUM, index=True)
    data_section = Column(DATA_SECTION_ENUM, index=True)
    camera = Column(CAMERA_ENUM, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):

        disperser = ad.disperser().for_db()
        if disperser in DISPERSERS:
            self.disperser = disperser

        self.filter_name = ad.filter_name().for_db()

        read_mode = ad.read_mode().for_db()
        if read_mode in READ_MODES:
            self.read_mode = read_mode

        well_depth = ad.well_depth_setting().for_db()
        if well_depth in WELL_DEPTHS:
            self.well_depth_setting = well_depth

        data_section = str(ad.data_section().for_db())
        if data_section in DATA_SECTIONS:
            self.data_section = data_section

        camera = ad.camera().for_db()
        if camera in CAMERAS:
            self.camera = camera

        self.focal_plane_mask = ad.focal_plane_mask().for_db()
