#
#                                                                    FitsStorage
#
#                                                             Gemini Observatory
#                                                     fits_store.orm.michelle.py
# ------------------------------------------------------------------------------
__version__      = '0.99 beta'
# ------------------------------------------------------------------------------
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Enum
from sqlalchemy.orm import relation

from . import Base
from .header import Header

READ_MODES = ['CHOP', 'NDCHOP', 'STARE', 'chop', 'stare', 'nod', 'chop-nod']
READ_MODE_ENUM = Enum(*READ_MODES, name='michelle_read_mode')

# Now that the instrument is decomissioned, we can emum these for efficiency
DISPERSERS = ['Echelle', 'LowN', 'LowQ', 'MedN1', 'MedN2', 'No Value', 'unknown']
DISPERSER_ENUM = Enum(*DISPERSERS, name='michelle_disperser')

FILTERS = ['blank', 'BothBlanks', 'Clear', 'Clear_A', 'F112B21', 'F116B9','F125B9',
           'F209B42L', 'F66LB', 'F86B2', 'I103B10', 'I105B53', 'I107B4', 'I112B21',
           'I116B9', 'I125B9', 'I128B2', 'I185B9', 'I198B27', 'I209B42', 'I79B10',
           'I86B2', 'I88B10', 'I97B10', 'IP103B10', 'IP112B21', 'IP116B9', 'IP125B9',
           'IP185B9', 'IP198B27', 'IP79B10', 'IP88B10', 'IP97B10', 'No Value',
           'NPBlock', 'Poly', 'QBlock']

FILTER_NAME_ENUM = Enum(*FILTERS, name='michelle_filter')

FOCAL_PLANE_MASKS = ['4_pixels', 'unknown', '8_pixels', 'No Value', '1_pixel',
                     '2_pixels', '3_pixels', '6_pixels', 'pinholeMask', '16_pixels',
                     'None']

FOCAL_PLANE_MAKE_ENUM = Enum(*FOCAL_PLANE_MASKS, name='michelle_focal_plane_mask')

# ------------------------------------------------------------------------------
class Michelle(Base):
    """
    This is the ORM object for the MICHELLE details.

    """
    __tablename__ = 'michelle'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    disperser = Column(DISPERSER_ENUM, index=True)
    filter_name = Column(FILTER_NAME_ENUM, index=True)
    read_mode = Column(READ_MODE_ENUM, index=True)
    focal_plane_mask = Column(FOCAL_PLANE_MAKE_ENUM, index=True)

    def __init__(self, header, ad):
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        disperser = ad.disperser()
        if disperser in DISPERSERS:
            self.disperser = disperser

        filter_name = ad.filter_name()
        if filter_name in FILTERS:
            self.filter_name = filter_name

        read_mode = ad.read_mode()
        if read_mode in READ_MODES:
            self.read_mode = read_mode

        focal_plane_mask = ad.focal_plane_mask()
        if focal_plane_mask in FOCAL_PLANE_MASKS:
            self.focal_plane_mask = focal_plane_mask

