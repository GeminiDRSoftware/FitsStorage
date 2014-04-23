from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Numeric
from sqlalchemy.orm import relation

from . import Base

class QAreport(Base):
    """
    This is the ORM class for a QA metric report.
    """
    __tablename__ = 'qareport'

    id = Column(Integer, primary_key=True)
    hostname = Column(Text)
    userid = Column(Text)
    processid = Column(Integer)
    executable = Column(Text)
    software = Column(Text)
    software_version = Column(Text)
    context = Column(Text)
    submit_time = Column(DateTime)
    submit_host = Column(Text)

class QAmetricIQ(Base):
    """
    This is the ORM class for a QA IQ metric measurement
    """
    __tablename__ = 'qametriciq'

    id = Column(Integer, primary_key=True)
    qareport_id = Column(Integer, ForeignKey('qareport.id'))
    qareport = relation(QAreport, order_by=id)
    datalabel = Column(Text)
    filename = Column(Text)
    detector = Column(Text)
    # Image Quality Values
    fwhm = Column(Numeric(precision=6, scale=3))
    fwhm_std = Column(Numeric(precision=6, scale=3))
    isofwhm = Column(Numeric(precision=6, scale=3))
    isofwhm_std = Column(Numeric(precision=6, scale=3))
    ee50d = Column(Numeric(precision=6, scale=3))
    ee50d_std = Column(Numeric(precision=6, scale=3))
    elip = Column(Numeric(precision=5, scale=3))
    elip_std = Column(Numeric(precision=5, scale=3))
    pa = Column(Numeric(precision=6, scale=3))
    pa_std = Column(Numeric(precision=6, scale=3))
    strehl = Column(Numeric(precision=6, scale=3))
    strehl_std = Column(Numeric(precision=6, scale=3))
    nsamples = Column(Integer)
    percentile_band = Column(Text)
    comment = Column(Text)

    def __init__(self, qareport):
        self.qareport_id = qareport.id


class QAmetricZP(Base):
    """
    This is the ORM class for a QA ZP metric measurement
    """
    __tablename__ = 'qametriczp'

    id = Column(Integer, primary_key=True)
    qareport_id = Column(Integer, ForeignKey('qareport.id'))
    qareport = relation(QAreport, order_by=id)
    datalabel = Column(Text)
    filename = Column(Text)
    detector = Column(Text)
    # Photometry
    mag = Column(Numeric(precision=5, scale=3))
    mag_std = Column(Numeric(precision=5, scale=3))
    cloud = Column(Numeric(precision=5, scale=3))
    cloud_std = Column(Numeric(precision=5, scale=3))
    photref = Column(Text)
    nsamples = Column(Integer)
    percentile_band = Column(Text)
    comment = Column(Text)

    def __init__(self, qareport):
        self.qareport_id = qareport.id


class QAmetricSB(Base):
    """
    This is the ORM class for a QA SB metric measurement
    """
    __tablename__ = 'qametricsb'

    id = Column(Integer, primary_key=True)
    qareport_id = Column(Integer, ForeignKey('qareport.id'))
    qareport = relation(QAreport, order_by=id)
    datalabel = Column(Text)
    filename = Column(Text)
    detector = Column(Text)
    # Sky Background
    mag = Column(Numeric(precision=5, scale=3))
    mag_std = Column(Numeric(precision=5, scale=3))
    electrons = Column(Numeric(precision=9, scale=2))
    electrons_std = Column(Numeric(precision=7, scale=2))
    nsamples = Column(Integer)
    percentile_band = Column(Text)
    comment = Column(Text)

    def __init__(self, qareport):
        self.qareport_id = qareport.id


class QAmetricPE(Base):
    """
    This is the ORM class for a QA PE (Astrometric Pointing Error) metric measurement
    """
    __tablename__ = 'qametricpe'

    id = Column(Integer, primary_key=True)
    qareport_id = Column(Integer, ForeignKey('qareport.id'))
    qareport = relation(QAreport, order_by=id)
    datalabel = Column(Text)
    filename = Column(Text)
    detector = Column(Text)
    # Astrometric Pointing Error
    dra = Column(Numeric(precision=9, scale=3))
    dra_std = Column(Numeric(precision=9, scale=3))
    ddec = Column(Numeric(precision=9, scale=3))
    ddec_std = Column(Numeric(precision=9, scale=3))
    astref = Column(Text)
    nsamples = Column(Integer)
    comment = Column(Text)

    def __init__(self, qareport):
        self.qareport_id = qareport.id


