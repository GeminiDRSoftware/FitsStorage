from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, DateTime, String
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from sqlalchemy.ext.associationproxy import association_proxy

import astrodata
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
    ad = diskfile.ad_object
    if 'GEM_PROVENANCE' in ad:
        provenance = ad.GEM_PROVENANCE
        prov_list = list()
        for row in provenance:
            timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            filename = row[1]
            md5 = row[2]
            primitive = row[3]
            prov_row = Provenance(timestamp, filename, md5, primitive)
            prov_list.append(prov_row)
        diskfile.provenance = prov_list
    if 'GEM_PROVENANCE_HISTORY' in ad:
        history = ad.GEM_PROVENANCE_HISTORY
        hist_list = list()
        for row in history:
            timestamp_start = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            timestamp_end = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
            primitive = row[2]
            args = row[3]
            hist_row = ProvenanceHistory(timestamp_start, timestamp_end, primitive, args)
            hist_list.append(hist_row)
        diskfile.provenance_history = hist_list
