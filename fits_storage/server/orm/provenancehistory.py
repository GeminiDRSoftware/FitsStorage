from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base


__all__ = ["Provenance", "History", "ingest_provenancehistory"]


PROVENANCE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
PROVENANCE_DATE_FORMAT_ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _parse_timestamp(ts_str):
    if 'T' in ts_str:
        return datetime.strptime(ts_str, PROVENANCE_DATE_FORMAT_ISO)
    else:
        return datetime.strptime(ts_str, PROVENANCE_DATE_FORMAT)


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
    added_by : str
        Name of the thing (usually a DRAGONS primitive) that added this
        provenance
 """
    __tablename__ = 'provenance'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    filename = Column(Text)
    md5 = Column(Text)
    added_by = Column(Text)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))
    diskfile = relationship("DiskFile", back_populates='provenance')

    def __init__(self, timestamp: datetime, filename: str, md5: str,
                 added_by: str):
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
        added_by : str
            Name of the thing (usually a DRAGONS primitive) that added this
            provenance
        """
        self.timestamp = timestamp
        self.filename = filename
        self.md5 = md5
        self.added_by = added_by


class History(Base):
    """
    This is the ORM class for storing  history details from the FITS file.
    """
    __tablename__ = 'history'

    id = Column(Integer, primary_key=True)
    timestamp_start = Column(DateTime)
    timestamp_end = Column(DateTime)
    primitive = Column(Text)
    args = Column(Text)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'))
    diskfile = relationship("DiskFile", back_populates='history')

    def __init__(self, timestamp_start: datetime, timestamp_end: datetime,
                 primitive: str, args: str):
        """
        Create a history record.

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


def ingest_provenancehistory(diskfile):
    """
    Ingest the provenance and history data from the diskfile into the database.
    These are rolled together into one function simply for convenience as we
    usually do both at the same time and there is some shared functionality
    in for example parsing timestamps.

    This helper method reads the FITS file to extract the
    :class:`~provenance.Provenance`
    and :class:`~provenance.History` data from it and ingest it
    into the database.

    Parameters
    ----------
    diskfile : :class:`~fits_storage_core.orm.diskfile.Diskfile`
        diskfile to read provenance data out of

    Returns
    -------
    None
    """

    ad = diskfile.ad_object
    if hasattr(ad, 'PROVENANCE'):
        provenance = ad.PROVENANCE
        if provenance:
            prov_list = list()
            for prov in provenance:
                timestamp = _parse_timestamp(prov[0])
                filename = prov[1]
                md5 = prov[2]
                added_by = prov[3]
                prov_row = Provenance(timestamp, filename, md5, added_by)
                prov_list.append(prov_row)
            diskfile.provenance = prov_list
    if hasattr(ad, 'PROVENANCE_HISTORY'):
        provenance_history = ad.PROVENANCE_HISTORY
        if provenance_history:
            hist_list = list()
            for ph in provenance_history:
                timestamp_start = _parse_timestamp(ph[0])
                timestamp_stop = _parse_timestamp(ph[1])
                primitive = ph[2]
                args = ph[3]
                hist = History(timestamp_start, timestamp_stop, primitive, args)
                hist_list.append(hist)
            diskfile.history = hist_list
