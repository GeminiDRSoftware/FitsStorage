from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from .header import Header

from . import Base

# ------------------------------------------------------------------------------
class F2(Base):
    """
    This is the ORM object for the F2 details

    """
    __tablename__ = 'f2'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    lyot_stop = Column(Text, index=True)
    read_mode = Column(Text, index=True)
    focal_plane_mask = Column(Text)

    def __init__(self, header, ad):
        """
        Create an F2 record with the given :class:`~header.Header` and data from :class:`astrodata.Astrodata`

        Parameters
        ----------
        header : :class:`~header.Header`
            Header record corresponding to this F2 instrument data record
        ad : :class:`astrodata.Astrodata`
            Astrodata object to parse for additional F2 data
        """
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate the F2 information from the given :class:`~astrodata.Astrodata` object

        Parameters
        ----------
        ad : :class:`astrodata.Astrodata`
            Astrodata object to read F2 information from
        """
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()
        self.lyot_stop = ad.lyot_stop()
        self.read_mode = ad.read_mode()
        self.focal_plane_mask = ad.focal_plane_mask()
