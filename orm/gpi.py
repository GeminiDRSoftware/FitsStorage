from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy.orm import relation

from orm.header import Header

from . import Base

class Gpi(Base):
    """
    This is the ORM object for the GPI details
    """
    __tablename__ = 'gpi'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    coadds = Column(Integer, index=True)
    filter_name = Column(Text, index=True)
    disperser = Column(Text, index=True)
    focal_plane_mask = Column(Text)
    astrometic_standard = Column(Boolean)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.coadds = ad.coadds().for_db()
        self.filter_name = ad.filter_name().for_db()
        self.disperser = ad.disperser().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
        self.astrometric_standard = self.get_astrometric_standard(ad)

    def get_astrometric_standard(self, ad):
        """
        It's the value of the ASTROMTC header from the first extension.
        """
        value = ad[1].get_key_value("ASTROMTC")
        return value
