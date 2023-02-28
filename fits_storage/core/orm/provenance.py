from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relationship

from . import Base


__all__ = ["Provenance", "ProvenanceHistory", "ingest_provenance"]

PROVENANCE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
PROVENANCE_DATE_FORMAT_ISO = "%Y-%m-%dT%H:%M:%S.%f"


class Provenance(Base):
    """
    This is the ORM class for storing provenance data found in the FITS file.

    Parameters
    ----------
    timestamp : datetime
        Time of the provenance occurring
    filename : str
        Name of the file involved
    md5 : str
        MD5 Checksum of the input file
    primitive : str
        Name of the DRAGONS primitive that was performed
    """
    __tablename__ = 'provenance'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    filename = Column(Text)
    md5 = Column(Text)
    primitive = Column(Text)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))
    #diskfile = relationship("DiskFile", back_populates="provenance")

    def __init__(self, timestamp: datetime, filename: str, md5: str,
                 primitive: str):
        """
        Create provenance record with the given information

        Parameters
        ----------
        timestamp : datetime
            Time of the provenance occurring
        filename : str
            Name of the file involved
        md5 : str
            MD5 Checksum of the input file
        primitive : str
            Name of the DRAGONS primitive that was performed
        """
        self.timestamp = timestamp
        self.filename = filename
        self.md5 = md5
        self.primitive = primitive


class ProvenanceHistory(Base):
    """
    This is the ORM class for storing provenance history details from the
    FITS file.
    """
    __tablename__ = 'provenance_history'

    id = Column(Integer, primary_key=True)
    timestamp_start = Column(DateTime)
    timestamp_end = Column(DateTime)
    primitive = Column(Text)
    args = Column(Text)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))
    #diskfile = relationship("DiskFile", back_populates="provenance_history")

    def __init__(self, timestamp_start: datetime, timestamp_end: datetime,
                 primitive: str, args: str):
        """
        Create a provenance history record.

        These are more fine-grained than the provenance in that it captures
        the arguments and the start and stop times

        Parameters
        ----------
        timestamp_start : datetime
            time the operation began
        timestamp_end : datetime
            time the operation completed
        primitive : str
            Name of the DRAGONS primitive performed
        args : str
            string-encoded arguments that were passed to the primitive
        """
        self.timestamp_start = timestamp_start
        self.timestamp_end = timestamp_end
        self.primitive = primitive
        self.args = args


def ingest_provenance(diskfile):
    """
    Ingest the provenance data from the diskfile into the database.

    This helper method reads the FITS file to extract the
    :class:`~provenance.Provenance`
    and :class:`~provenance.ProvenanceHistory` data from it and ingest it
    into the database.

    Parameters
    ----------
    diskfile : :class:`~fits_storage_core.orm.diskfile.Diskfile`
        diskfile to read provenance data out of

    Returns
    -------
    None
    """
    def _parse_timestamp(ts_str):
        if 'T' in ts_str:
            return datetime.strptime(timestamp_str, PROVENANCE_DATE_FORMAT_ISO)
        else:
            return datetime.strptime(timestamp_str, PROVENANCE_DATE_FORMAT)
    ad = diskfile.ad_object
    if hasattr(ad, 'PROVENANCE'):
        provenance = ad.PROVENANCE
        if provenance:
            prov_list = list()
            for prov in provenance:
                timestamp_str = prov[0]
                timestamp = _parse_timestamp(timestamp_str)
                filename = prov[1]
                md5 = prov[2]
                provenance_added_by = prov[3]
                prov_row = Provenance(timestamp, filename, md5,
                                      provenance_added_by)
                prov_list.append(prov_row)
            diskfile.provenance = prov_list
    if hasattr(ad, 'PROVENANCE_HISTORY'):
        provenance_history = ad.PROVENANCE_HISTORY
        if provenance_history:
            hist_list = list()
            for ph in provenance_history:
                timestamp_start_str = ph[0]
                timestamp_stop_str = ph[1]
                timestamp_start = _parse_timestamp(timestamp_start_str)
                timestamp_stop = _parse_timestamp(timestamp_stop_str)
                primitive = ph[2]
                args = ph[3]
                hist = ProvenanceHistory(timestamp_start, timestamp_stop,
                                         primitive, args)
                hist_list.append(hist)
            diskfile.provenance_history = hist_list
