from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text
from fits_storage.core.header import Header

from fits_storage.core import Base


class LogComments(Base):
    """
    This is the ORM class for the table containing obslog comments for images
    (extracted from ODB).

    """
    __tablename__ = 'logcomments'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey(Header.id), nullable=False, index=True)
    comment = Text()

    def __init__(self, header, comment=None):
        """
        Initialize a new :class:`~LogComments` record for the given header

        Parameters
        ----------
        header : :class:`~header.Header`
            Header record to build :class:`~LogComments` for
        comment : str
            Optional comment to put in this record
        """
        self.header_id = header.id
        if comment is not None:
            self.comment = comment
