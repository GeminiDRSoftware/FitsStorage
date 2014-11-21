from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from orm.header import Header

from . import Base

class Nici(Base):
    """
    This is the ORM object for the NICI details
    """
    __tablename__ = 'nici'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    filter_name = Column(Text, index=True)
    focal_plane_mask = Column(Text, index=True)
    disperser = Column(Text, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.filter_name = ad.filter_name().for_db()
        self.focal_plane_mask = ad.focal_plane_mask().for_db()
        self.disperser = ad.disperser().for_db()
