from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, Enum
from sqlalchemy.orm import relation

from .header import Header

from . import Base

# Enumerated column types
READ_SPEED_SETTINGS = ['slow', 'fast']
READ_SPEED_SETTING_ENUM = Enum(*READ_SPEED_SETTINGS, name='ghost_read_speed_setting')

GAIN_SETTINGS = ['low', 'high']
GAIN_SETTING_ENUM = Enum(*GAIN_SETTINGS, name='ghost_gain_setting')

RESOLUTIONS = ['std', 'high']
RESOLUTION_ENUM = Enum(*RESOLUTIONS, name='ghost_resolution')


class Ghost(Base):
    """
    This is the ORM object for the GHOST details.
    """
    __tablename__ = 'ghost'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    detector_name = Column(Text, index=True)
    detector_x_bin = Column(Integer, index=True)
    detector_y_bin = Column(Integer, index=True)
    amp_read_area = Column(Text, index=True)
    read_speed_setting = Column(READ_SPEED_SETTING_ENUM, index=True)
    gain_setting = Column(GAIN_SETTING_ENUM, index=True)
    res_mode = Column(RESOLUTION_ENUM, index=True)
    focal_plane_mask = Column(Text, index=True)
    prepared = Column(Boolean, index=True)
    overscan_subtracted = Column(Boolean, index=True)
    overscan_trimmed = Column(Boolean, index=True)

    def __init__(self, header, ad):
        """
        Create a Ghost instrument record

        Parameters
        ----------
        header : :class:`~header.Header`
            Corresponding header for the observation
        ad : :class:`astrodata.AstroData`
            Astrodata object to load Ghost information from
        """
        self.header = header

        # Populate from the astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate the Ghost information from the given :class:`astrodata.AstroData`

        Parameters
        ----------
        ad : :class:`astrodata.Astrodata`
            Astrodata object to read Ghost information from
        """
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()
        try:
            self.detector_x_bin = ad.detector_x_bin()
            self.detector_y_bin = ad.detector_y_bin()
            self.amp_read_area = '+'.join(ad.amp_read_area())
            self.detector_name = ad.detector_name()

            gain_setting = ad.gain_setting()
            if gain_setting in GAIN_SETTINGS:
                self.gain_setting = gain_setting
        except AssertionError:
            # Likely an MDF file. There are no pixel extensions for
            # that, and we'll get a horrible exception trying to get
            # to those elements.
            pass

        read_speed = ad.read_speed_setting()
        if read_speed in READ_SPEED_SETTINGS:
            self.read_speed_setting = read_speed

        resolution = ad.res_mode()
        if resolution in RESOLUTIONS:
            self.res_mode = resolution

        self.focal_plane_mask = ad.focal_plane_mask()
        self.prepared = 'PREPARED' in ad.tags
        self.overscan_trimmed = 'OVERSCAN_TRIMMED' in ad.tags
        self.overscan_subtracted = 'OVERSCAN_SUBTRACTED' in ad.tags
