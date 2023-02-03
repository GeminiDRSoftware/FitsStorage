from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation

from gemini_obs_db.orm import Base
from .header import Header


__all__ = ["Nifs"]


READ_MODES = ['Faint Object', 'Medium Object', 'Bright Object', 'Invalid']
READ_MODE_ENUM = Enum(*READ_MODES, name='nifs_read_mode')


class Nifs(Base):
    """
    This is the ORM object for the NIFS details

    Parameters
    ----------
    header : :class:`~gemini_obs_db.orm.header.Header`
        Header record linked to this one
    ad : :class:`~astrodata.core.AstroData`
        AstroData object to read NIFS data from
    """
    __tablename__ = 'nifs'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    read_mode = Column(READ_MODE_ENUM, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header, ad):
        """
        Create a record for NIFS data linked to the given header and sourced
        from an :class:`~astrodata.core.AstroData` object

        Parameters
        ----------
        header : :class:`~gemini_obs_db.orm.header.Header`
            Header record linked to this one
        ad : :class:`~astrodata.core.AstroData`
            AstroData object to read NIFS data from
        """
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate the NIFS record data from an :class:`~astrodata.core.AstroData` object

        Parameters
        ----------
        ad : :class:`~astrodata.core.AstroData`
            AstroData object to read NIFS data from
        """
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()

        read_mode = ad.read_mode()
        if read_mode in READ_MODES:
            self.read_mode = read_mode

        self.focal_plane_mask = ad.focal_plane_mask()
