from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum
from sqlalchemy.orm import relation

from . import Base
from .header import Header

FOCAL_PLANE_MASKS = ['Clear_G5710', 'F0.22_G5715', 'F0.32_G5714', 'F0.46_G5713', 
                     'F0.65_G5712', 'F0.90_G5711', 'Grid_G5716']
FOCAL_PLANE_MASK_ENUM = Enum(*FOCAL_PLANE_MASKS, name='nici_focal_plane_mask')

DISPERSERS = ['Block', 'H-50/50_G5701', 'H-CH4-Dichroic_G5704', 
              'H/K-Dichroic_G5705', 'Mirror_G5702', 'Open']
DISPERSER_ENUM = Enum(*DISPERSERS, name='nici_disperser')


class Nici(Base):
    """
    This is the ORM object for the NICI details
    """
    __tablename__ = 'nici'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    filter_name = Column(Text, index=True)
    focal_plane_mask = Column(FOCAL_PLANE_MASK_ENUM, index=True)
    disperser = Column(DISPERSER_ENUM, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        self.filter_name = ad.filter_name()
      
        focal_plane_mask = ad.focal_plane_mask()
        if focal_plane_mask in FOCAL_PLANE_MASKS:
            self.focal_plane_mask = focal_plane_mask

        disperser = ad.disperser()
        if disperser in DISPERSERS:
            self.disperser = disperser

