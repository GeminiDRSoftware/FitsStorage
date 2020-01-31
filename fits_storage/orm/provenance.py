import json
from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, String

from . import Base


# matches the value in DRAGONS
# avoiding import within orm to avoid any circular referencing that could happen in future
PROVENANCE_DATE_FORMAT="%Y-%m-%d %H:%M:%S.%f"


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
    if hasattr(ad, 'PROVENANCE'):
        provenance = ad.PROVENANCE
        if provenance:
            prov_list = list()
            for prov in provenance:
                timestamp_str = prov[0]
                timestamp = datetime.strptime(timestamp_str, PROVENANCE_DATE_FORMAT)
                filename = prov[1]
                md5 = prov[2]
                provenance_added_by = prov[3]
                prov_row = Provenance(timestamp, filename, md5, provenance_added_by)
                prov_list.append(prov_row)
            diskfile.provenance = prov_list
    if hasattr(ad, 'PROVENANCE_HISTORY'):
        provenance_history = ad.PROVENANCE_HISTORY
        if provenance_history:
            hist_list = list()
            for ph in provenance_history:
                timestamp_start_str = ph[0]
                timestamp_stop_str = ph[1]
                timestamp_start = datetime.strptime(timestamp_start_str, PROVENANCE_DATE_FORMAT)
                timestamp_stop = datetime.strptime(timestamp_stop_str, PROVENANCE_DATE_FORMAT)
                primitive = ph[2]
                args = ph[3]
                hist = ProvenanceHistory(timestamp_start, timestamp_stop, primitive, args)
                hist_list.append(hist)
            diskfile.provenance_history = hist_list
