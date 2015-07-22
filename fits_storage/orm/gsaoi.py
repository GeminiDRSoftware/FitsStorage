from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from sqlalchemy.orm import relation

from . import Base
from .header import Header

class Gsaoi(Base):
    """
    This is the ORM object for the GSAOI details
    """
    __tablename__ = 'gsaoi'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    filter_name = Column(Text, index=True)
    read_mode = Column(Text, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.filter_name = ad.filter_name().for_db()
        self.read_mode = ad.read_mode().for_db()
