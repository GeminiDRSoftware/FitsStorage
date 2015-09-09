from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, Enum
from sqlalchemy.orm import relation
from sqlalchemy.exc import DataError

from .header import Header

from . import Base

# Enumerated column types
READ_SPEED_SETTING_ENUM = Enum('slow', 'fast', name='gmos_read_speed_setting')
GAIN_SETTING_ENUM = Enum('low', 'high', name='gmos_gain_setting')

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
    read_speed_setting = Column(READ_SPEED_SETTING_ENUM, index=True)
    gain_setting = Column(GAIN_SETTING_ENUM, index=True)
    focal_plane_mask = Column(Text, index=True)
    nodandshuffle = Column(Boolean, index=True)
    nod_count = Column(Integer, index=True)
    nod_pixels = Column(Integer, index=True)
    prepared = Column(Boolean, index=True)
    overscan_subtracted = Column(Boolean, index=True)
    overscan_trimmed = Column(Boolean, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from the astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.disperser = ad.disperser().for_db()
        self.filter_name = ad.filter_name().for_db()
        self.detector_x_bin = ad.detector_x_bin().for_db()
        self.detector_y_bin = ad.detector_y_bin().for_db()
        self.amp_read_area = ad.amp_read_area().for_db()

        try:
            self.read_speed_setting = ad.read_speed_setting().for_db()
        except DataError:
            pass

        try:
            self.gain_setting = ad.gain_setting().for_db()
        except DataError:
            pass

        self.focal_plane_mask = ad.focal_plane_mask().for_db()
        self.nodandshuffle = 'GMOS_NODANDSHUFFLE' in ad.types
        if self.nodandshuffle:
            self.nod_count = ad.nod_count().for_db()
            self.nod_pixels = ad.nod_pixels().for_db()
        self.prepared = 'PREPARED' in ad.types
        self.overscan_trimmed = 'OVERSCAN_TRIMMED' in ad.types
        self.overscan_subtracted = 'OVERSCAN_SUBTRACTED' in ad.types
