from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Numeric
from sqlalchemy.orm import relationship

from . import Base
from .footprint import Footprint

from decimal import Decimal

class PhotStandard(Base):
    """
    This is the ORM class for the table holding the standard star list for
    the instrument monitoring.

    """
    __tablename__ = 'photstandard'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    field = Column(Text)
    ra = Column(Numeric(precision=16, scale=12), index=True)
    dec = Column(Numeric(precision=16, scale=12), index=True)
    u_mag = Column(Numeric(precision=6, scale=4))
    v_mag = Column(Numeric(precision=6, scale=4))
    g_mag = Column(Numeric(precision=6, scale=4))
    r_mag = Column(Numeric(precision=6, scale=4))
    i_mag = Column(Numeric(precision=6, scale=4))
    z_mag = Column(Numeric(precision=6, scale=4))
    y_mag = Column(Numeric(precision=6, scale=4))
    j_mag = Column(Numeric(precision=6, scale=4))
    h_mag = Column(Numeric(precision=6, scale=4))
    k_mag = Column(Numeric(precision=6, scale=4))
    lprime_mag = Column(Numeric(precision=6, scale=4))
    m_mag = Column(Numeric(precision=6, scale=4))

    def as_dict(self):
        """
        Return a dict representation of this photstd
        """
        d = {}
        for item in ['name', 'field', 'ra', 'dec', 'u_mag', 'v_mag', 'g_mag',
                     'r_mag', 'i_mag', 'z_mag', 'y_mag', 'j_mag', 'h_mag',
                     'k_mag', 'lprime_mag', 'm_mag']:
            thing = getattr(self, item)
            if isinstance(thing, Decimal):
                thing = float(thing)
            d[item] = thing

        return d

class PhotStandardObs(Base):
    """
    This is the ORM class for the table detailing which standard stars are
    observed in which headers.

    """
    __tablename__ = "photstandardobs"

    id = Column(Integer, primary_key=True)
    photstandard_id = Column(Integer, ForeignKey(PhotStandard.id),
                             nullable=False, index=True)
    footprint_id = Column(Integer, ForeignKey(Footprint.id), nullable=False,
                          index=True)
    photstandard = relationship(PhotStandard, order_by=id)
    footprint = relationship(Footprint, order_by=id)

