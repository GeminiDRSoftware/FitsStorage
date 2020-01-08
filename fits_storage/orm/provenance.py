import json
from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, String

from . import Base


class Provenance(Base):
    """
    This is the ORM class for storing provenance data found in the FITS file.
    """
    __tablename__ = 'provenance'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    filename = Column(String(128))
    md5 = Column(Text)
    primitive = Column(String(128))
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))

    def __init__(self, timestamp, filename, md5, primitive):
        self.timestamp = timestamp
        self.filename = filename
        self.md5 = md5
        self.primitive = primitive


class ProvenanceHistory(Base):
    """
    This is the ORM class for storing provenance history details from the FITS file.
    """
    __tablename__ = 'provenance_history'

    id = Column(Integer, primary_key=True)
    timestamp_start = Column(DateTime)
    timestamp_end = Column(DateTime)
    primitive = Column(String(128))
    args = Column(Text)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))

    def __init__(self, timestamp_start, timestamp_end, primitive, args):
        self.timestamp_start = timestamp_start
        self.timestamp_end = timestamp_end
        self.primitive = primitive
        self.args = args


def ingest_provenance(diskfile):
    """
    ingest the provenance data from the diskfile into the database.

    :param diskfile: diskfile to read provenance data out of
    :return: none
    """
    ad = diskfile.ad_object
    if hasattr(ad, 'provenance'):
        provenance = ad.provenance
        if provenance:
            prov_list = list()
            for prov in provenance:
                timestamp = prov.timestamp
                filename = prov.filename
                md5 = prov.md5
                provenance_added_by = prov.provenance_added_by
                prov_row = Provenance(timestamp, filename, md5, provenance_added_by)
                prov_list.append(prov_row)
            diskfile.provenance = prov_list
    if hasattr(ad, 'provenance_history'):
        provenance_history = ad.provenance_history
        if provenance_history:
            hist_list = list()
            for ph in provenance_history:
                timestamp_start = ph.timestamp_start
                timestamp_stop = ph.timestamp_stop
                primitive = ph.primitive
                args = ph.args
                hist = ProvenanceHistory(timestamp_start, timestamp_stop, primitive, args)
                hist_list.append(hist)
            diskfile.provenance_history = hist_list
