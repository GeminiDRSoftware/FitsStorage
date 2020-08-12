from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from . import Base
from .header import Header


class Gsaoi(Base):
    """
    This is the ORM object for the GSAOI details.

    """
    __tablename__ = 'gsaoi'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    filter_name = Column(Text, index=True)
    read_mode = Column(Text, index=True)

    def __init__(self, header, ad):
        """
        Create a record for GSAOI information

        Parameters
        ----------
        header : :class:`~header.Header`
            Header record connected to this information
        ad : :class:`astrodata.AstroData`
            AstroData to parse for GSAOI information
        """
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate the GSAOI information from the astrodata object

        Parameters
        ----------
        ad : :class:`astrodata.AstroData`
            AstroData object to populate GSAOI information from
        """
        self.filter_name = ad.filter_name()
        self.read_mode = ad.read_mode()
