from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Numeric
from sqlalchemy.orm import relation

from gemini_obs_db import Base
from .footprint import Footprint


class PhotStandard(Base):
    """
    This is the ORM class for the table holding the standard star list for the
    instrument monitoring.

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


class PhotStandardObs(Base):
    """
    This is the ORM class for the table detailing which standard stars are observed
    in which headers.

    """
    __tablename__ = "photstandardobs"

    id = Column(Integer, primary_key=True)
    photstandard_id = Column(Integer, ForeignKey('photstandard.id'), nullable=False, index=True)
    footprint_id = Column(Integer, ForeignKey('footprint.id'), nullable=False, index=True)
    photstandard = relation(PhotStandard, order_by=id)
    footprint = relation(Footprint, order_by=id)



