import numpy
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Float, String

from fits_storage.core.orm import Base

from fits_storage.logger_dummy import DummyLogger


class Refcat(Base):
    """
    This is the ORM object for the refcat table, that captures entries from
    .REFCAT tables in reduced imaging data.
    """

    __tablename__ = 'refcat'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                         index=True)

    refcatid = Column(Integer)  # Id column from REFCAT table

    # Following are all simply column names from the REFCAT table

    Cat_Id = Column(String)

    RAJ2000 = Column(Float)
    DEJ2000 = Column(Float)

    umag = Column(Float)
    umag_err = Column(Float)
    gmag = Column(Float)
    gmag_err = Column(Float)
    rmag = Column(Float)
    rmag_err = Column(Float)
    imag = Column(Float)
    imag_err = Column(Float)
    zmag = Column(Float)
    zmag_err = Column(Float)
    filtermag = Column(Float)
    filtermag_err = Column(Float)

    def __init__(self, header_id, row, logger=DummyLogger()):
        # Populate refcat columns from a header_id and refcat table row
        self.header_id = header_id

        # Somewhat brute force approach.
        for key in row.keys():
            value = row.get(key)
            # Rename the REFCAT id column to avoid clash with table primary key
            key = 'refcatid' if key == 'Id' else key
            if hasattr(self, key):
                # Need to do some type munging...
                if isinstance(value, numpy.integer):
                    value = int(value)
                elif isinstance(value, numpy.floating):
                    value = None if numpy.isnan(value) else float(value)
                elif isinstance(value, str):
                    pass
                else:
                    logger.debug(f"Did not adapt type for refcat {key}")
                self.__setattr__(key, value)
            else:
                logger.warning(f"REFCAT table has column {key} that is not "
                               f"present in refcat ORM instance")
