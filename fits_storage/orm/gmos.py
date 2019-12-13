from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, Enum
from sqlalchemy.orm import relation

from .header import Header

from . import Base

# ------------------------------------------------------------------------------
# Enumerated column types
READ_SPEED_SETTINGS = ['slow', 'fast']
READ_SPEED_SETTING_ENUM = Enum(*READ_SPEED_SETTINGS, name='gmos_read_speed_setting')

GAIN_SETTINGS = ['low', 'high']
GAIN_SETTING_ENUM = Enum(*GAIN_SETTINGS, name='gmos_gain_setting')

# ------------------------------------------------------------------------------
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
    amp_read_area  = Column(Text, index=True)
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
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()
        try:
            self.detector_x_bin = ad.detector_x_bin()
            self.detector_y_bin = ad.detector_y_bin()
            self.amp_read_area = '+'.join(ad.amp_read_area())

            gain_setting = ad.gain_setting()
            if gain_setting in GAIN_SETTINGS:
                self.gain_setting = gain_setting
        except TypeError:
            # likely caused by poor metadata.
            pass
        except Exception:
            # Likely an MDF file. There are no pixel extensions for
            # that, and we'll get a horrible exception trying to get
            # to those elements.
            pass

        try:
            read_speed = ad.read_speed_setting()
        except AttributeError as ae:
            read_speed = None
        if read_speed in READ_SPEED_SETTINGS:
            self.read_speed_setting = read_speed

        self.focal_plane_mask = ad.focal_plane_mask()
        self.nodandshuffle = 'NODANDSHUFFLE' in ad.tags
        if self.nodandshuffle:
            self.nod_count = ad.nod_count()[0]
            self.nod_pixels = ad.shuffle_pixels()
        self.prepared = 'PREPARED' in ad.tags
        self.overscan_trimmed = 'OVERSCAN_TRIMMED' in ad.tags
        self.overscan_subtracted = 'OVERSCAN_SUBTRACTED' in ad.tags
