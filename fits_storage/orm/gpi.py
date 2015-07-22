from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy.orm import relation

from . import Base
from .header import Header

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
    focal_plane_mask = Column(Text, index=True)
    astrometric_standard = Column(Boolean, index=True)
    wollaston = Column(Boolean, index=True)
    prism = Column(Boolean, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.coadds = ad.coadds().for_db()
        self.filter_name = ad.filter_name().for_db()
        self.disperser = ad.disperser().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
        self.astrometric_standard = ad.phu_get_key_value("ASTROMTC")
        if self.disperser is not None:
            self.wollaston = 'WOLLASTON' in self.disperser
            self.prism = 'PRISM' in self.disperser 
