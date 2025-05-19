import numpy as np

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

    keyword = Column(Text, index=True)
    label = Column(Text, index=True)
    adid = Column(Integer)
    value_int = Column(Integer)
    value_float = Column(Float)
    value_text = Column(Text)
    notes = Column(Text)

    def __init__(self, ad):
        """
        Instantiate a monitoring table entry from an astrodata instance.
        This attempts to set the filename, data_label, ut_datetime,
        software_used, software_version and processing_tag, and if possible
        the adid (ad.id) value from the ad instance (ie FITS headers).
        """

        if ad is not None:
            self.filename = ad.filename
            self.data_label = ad.data_label()
            self.ut_datetime = ad.ut_datetime()
            self.software_used = ad.phu.get('PROCSOFT')
            self.software_version = ad.phu.get('PROCSVER')
            self.processing_tag = ad.phu.get('PROCTAG')

            try:
                self.adid = ad.id
            except ValueError:
                pass

    def set_value(self, value):
        if isinstance(value, (int, np.integer)):
            self.value_int = value
        elif isinstance(value, (float, np.floating)):
            self.value_float = value
        else:
            self.value_text = str(value)

    def get_value(self):
        # Only one of the values should be not null
        if self.value_int is not None:
            return self.value_int
        if self.value_float is not None:
            return self.value_float
        if self.value_text is not None:
            return self.value_text
        return None