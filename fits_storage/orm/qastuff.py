from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Numeric, Boolean
from sqlalchemy.orm import relation, relationship

from . import Base

import json

report_fields = (
    'hostname', 'userid', 'processid', 'executable', 'software',
    'software_version', 'context', 'submit_time', 'submit_host'
    )

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

    iq_metrics = relationship("QAmetricIQ")
    sb_metrics = relationship("QAmetricSB")
    zp_metrics = relationship("QAmetricZP")
    pe_metrics = relationship("QAmetricPE")

    @staticmethod
    def from_dict(qa_dict, host, time):
        qa = QAreport()
        qa.hostname = qa_dict['hostname']
        qa.userid = qa_dict['userid']
        qa.processid = qa_dict['processid']
        qa.executable = qa_dict['executable']
        qa.software = qa_dict['software']
        qa.software_version = qa_dict['software_version']
        qa.context = qa_dict['context']
        qa.submit_host = host
        qa.submit_time = time

        for qametric_dict in qa_dict['qametric']:
            # TODO: Check this with test data, because the previous XML ingesting
            #       code suggest that the qametric_dict contains *lists* of metrics
            if 'sb' in qametric_dict:
                qa.sb_metrics.append(QAmetricSB.from_metrics(qa, qametric_dict))
            if 'iq' in qametric_dict:
                qa.iq_metrics.append(QAmetricIQ.from_metrics(qa, qametric_dict))
            if 'zp' in qametric_dict:
                qa.zp_metrics.append(QAmetricZP.from_metrics(qa, qametric_dict))
            if 'pe' in qametric_dict:
                qa.pe_metrics.append(QAmetricPE.from_metrics(qa, qametric_dict))

        return qa

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except (AttributeError, TypeError):
            raise KeyError(key)

    def to_dict(self):
        return dict((key, self[key]) for key in report_fields)

iq_fields = (
    'datalabel', 'filename', 'detector',
    'fwhm', 'fwhm_std', 'isofwhm', 'isofwhm_std', 'ee50d', 'ee50d_std',
    'elip', 'elip_std', 'pa', 'pa_std', 'adaptive_optics', 'ao_seeing',
    'strehl', 'strehl_std', 'nsamples', 'percentile_band',
    'comment'
    )

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
    adaptive_optics = Column(Boolean)
    ao_seeing = Column(Numeric(precision=6, scale=3))
    strehl = Column(Numeric(precision=6, scale=3))
    strehl_std = Column(Numeric(precision=6, scale=3))
    nsamples = Column(Integer)
    percentile_band = Column(Text)
    comment = Column(Text)

    def __init__(self, qareport):
        self.qareport_id = qareport.id

    @staticmethod
    def from_metrics(report, metrics):
        iq = QAmetricIQ(report)
        iq.filename = metrics['filename']
        iq.datalabel = metrics['datalabel']
        iq.detector = metrics['detector']

        iq_dict = metrics['iq']
        iq.fwhm = iq_dict.get('fwhm')
        iq.fwhm_std = iq_dict.get('fwhm_std')
        iq.isofwhm = iq_dict.get('isofwhm')
        iq.isofwhm_std = iq_dict.get('isofwhm_std')
        iq.ee50d = iq_dict.get('ee50d')
        iq.ee50d_std = iq_dict.get('ee50d_std')
        iq.elip = iq_dict.get('elip')
        iq.elip_std = iq_dict.get('elip_std')
        iq.pa = iq_dict.get('pa')
        iq.pa_std = iq_dict.get('pa_std')
        iq.nsamples = iq_dict.get('nsamples')
        iq.percentile_band = iq_dict.get('percentile_band')
        iq.comment = ", ".join(iq_dict.get('comment'))
        iq.ao_seeing = iq_dict.get('ao_seeing')
        iq.adaptive_optics = iq_dict.get('adaptive_optics')

        return iq

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except (AttributeError, TypeError):
            raise KeyError(key)

    def keys(self):
        return iq_fields

    def to_dict(self):
        return dict((key, self[key]) for key in iq_fields)

    def to_json(self):
        return json.dumps(self.to_dict())

zp_fields = (
    'datalabel', 'filename', 'detector',
    'mag', 'mag_std', 'cloud', 'cloud_std', 'photref', 'nsamples', 'percentile_band',
    'comment'
    )

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

    @staticmethod
    def from_metrics(report, metrics):
        zp = QAmetricZP(report)
        zp.filename = metrics['filename']
        zp.datalabel = metrics['datalabel']
        zp.detector = metrics['detector']

        zp_dict = metrics['zp']
        zp.mag = zp_dict.get('mag')
        zp.mag_std = zp_dict.get('mag_std')
        zp.cloud = zp_dict.get('cloud')
        zp.cloud_std = zp_dict.get('cloud_std')
        zp.nsamples = zp_dict.get('nsamples')
        zp.photref = zp_dict.get('photref')
        zp.percentile_band = zp_dict.get('percentile_band')
        zp.comment = ", ".join(zp_dict.get('comment'))

        return zp

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except (AttributeError, TypeError):
            raise KeyError(key)

    def keys(self):
        return zp_fields

    def to_dict(self):
        return dict((key, self[key]) for key in zp_fields)

    def to_json(self):
        return json.dumps(self.to_dict())

sb_fields = (
    'datalabel', 'filename', 'detector',
    'mag', 'mag_std', 'electrons', 'electrons_std', 'nsamples', 'percentile_band',
    'comment'
    )

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

    @staticmethod
    def from_metrics(report, metrics):
        sb = QAmetricSB(report)
        sb.filename = metrics['filename']
        sb.datalabel = metrics['datalabel']
        sb.detector = metrics['detector']

        sb_dict = metrics['sb']
        sb.comment = ", ".join(sb_dict.get('comment'))

        # Sometimes the QAP sends unreasonable values which
        # exceed the data type definition acceptable ranges
        if sb_dict.get('mag') < 100:
            sb.mag = sb_dict.get('mag')
        if sb_dict.get('mag_std') < 100:
            sb.mag_std = sb_dict.get('mag_std')
        if sb_dict.get('electrons') < 100000:
            sb.electrons = sb_dict.get('electrons')
        if sb_dict.get('electrons_std') < 100000:
            sb.electrons_std = sb_dict.get('electrons_std')
        sb.nsamples = sb_dict.get('nsamples')
        sb.percentile_band = sb_dict.get('percentile_band')

        return sb

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except (AttributeError, TypeError):
            raise KeyError(key)

    def keys(self):
        return sb_fields

    def to_dict(self):
        return dict((key, self[key]) for key in sb_fields)

    def to_json(self):
        return json.dumps(self.to_dict())

pe_fields = (
    'datalabel', 'filename', 'detector',
    'dra', 'dra_std', 'ddec', 'ddec_std', 'astref', 'nsamples',
    'comment'
    )

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

    @staticmethod
    def from_metrics(report, metrics):
        pe = QAmetricPE(report)
        pe.filename = metrics['filename']
        pe.datalabel = metrics['datalabel']
        pe.detector = metrics['detector']
        pe_dict = metrics['pe']
        pe.astref = pe_dict.get('astref')
        pe.dra = pe_dict.get('dra')
        pe.dra_std = pe_dict.get('dra_std')
        pe.ddec = pe_dict.get('ddec')
        pe.ddec_std = pe_dict.get('ddec_std')
        pe.nsamples = pe_dict.get('nsamples')

        return pe

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except (AttributeError, TypeError):
            raise KeyError(key)

    def keys(self):
        return pe_fields

    def to_dict(self):
        return dict((key, self[key]) for key in pe_fields)

    def to_json(self):
        return json.dumps(self.to_dict())
