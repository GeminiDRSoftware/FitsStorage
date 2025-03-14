from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, BigInteger, Text, DateTime, Float

from fits_storage.core.orm import Base

class Monitoring(Base):
    """
    This is the ORM object for the monitoring table, which stores instrument
    (and site?) monitoring data extracted from reduced data.

    We record the source of the data as: filename, data_label and ut_datetime.

    We include a header_id ForeignKey column, but it *is* nullable - this
    allows this table to reference files which are not ingested in the
    database, but this is useful for referencing against things like filter
    or other instrument configurations. It is anticipated that it will be
    necessary at times to re-populate this header_id column from a lookup
    based on filename.

    We also record various parameter of the processing (software, version, tag)

    The payload here is: label, ext, value, notes.
    The label should be a short label of what the data represent, ext is
    the ad[n] index where it came from. value actually has several columns to
    facilitate different data types: value_int, value_float, value_text. There
    is also a text notes column.
    """

    __tablename__ = 'monitoring'

    id = Column(Integer, primary_key=True)

    filename = Column(Text, index=True)
    data_label = Column(Text, index=True)
    ut_datetime = Column(DateTime(timezone=False), index=True)
    header_id = Column(BigInteger, ForeignKey('header.id'), index=True)

    software_used = Column(Text)
    software_version = Column(Text)
    processing_tag = Column(Text, index=True)

    label = Column(Text, index=True)
    ext = Column(Integer)
    value_int = Column(Integer)
    value_float = Column(Float)
    value_text = Column(Text)
    notes = Column(Text)
