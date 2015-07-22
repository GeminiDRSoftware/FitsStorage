from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from .header import Header

from . import Base

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
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.disperser = ad.disperser().for_db()
        self.filter_name = ad.filter_name().for_db()
        self.lyot_stop = ad.lyot_stop().for_db()
        self.read_mode = ad.read_mode().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
