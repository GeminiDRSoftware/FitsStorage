from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation

from fits_storage.core.orm import Base
from fits_storage.core.orm.header import Header


__all__ = ["Gnirs"]


# Enumerated Column types
READ_MODES = ['Very Faint Objects', 'Faint Objects', 'Bright Objects',
              'Very Bright Objects', 'Invalid']

READ_MODE_ENUM = Enum(*READ_MODES, name='gnirs_read_mode')
WELL_DEPTH_SETTINGS = ['Shallow', 'Deep', 'Invalid']
WELL_DEPTH_SETTING_ENUM = Enum(*WELL_DEPTH_SETTINGS,
                               name='gnirs_well_depth_setting')


class Gnirs(Base):
    """
    This is the ORM object for the GNIRS details.

    Parameters
    ----------
    header : :class:`~fits_storage_core.orm.header.Header`
        Header corresponding to this record
    ad : :class:`~astrodata.core.AstroData`
        AstroData object to read GNIRS information from
    """
    __tablename__ = 'gnirs'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                       index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(READ_MODE_ENUM, index=True)
    well_depth_setting = Column(WELL_DEPTH_SETTING_ENUM, index=True)
    camera = Column(Text, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header: Header, ad):
        """
        Create a GNIRS record for the given header and astrodata

        Parameters
        ----------
        header : :class:`~fits_storage_core.orm.header.Header`
            Header corresponding to this record
        ad : :class:`~astrodata.core.AstroData`
            AstroData object to read GNIRS information from
        """
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate this GNIRS record from the given astrodata

        Parameters
        ----------
        ad : :class:`~astrodata.core.AstroData`
            AstroData object to populate from
        """
        try:
            self.disperser = ad.disperser()
        except AttributeError as ae:
            self.disperser = None
        self.filter_name = ad.filter_name()

        read_mode = ad.read_mode()
        if read_mode in READ_MODES:
            self.read_mode = read_mode

        well_depth_setting = ad.well_depth_setting()
        if well_depth_setting in WELL_DEPTH_SETTINGS:
            self.well_depth_setting = well_depth_setting

        self.camera = ad.camera()
        self.focal_plane_mask = ad.focal_plane_mask()
