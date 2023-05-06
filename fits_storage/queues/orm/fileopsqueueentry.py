"""
This module contains the FileopsQueueEntry ORM class.

"""
import datetime

from sqlalchemy import Column
from sqlalchemy import Integer, Boolean, Text, DateTime

from fits_storage.core.orm import Base
from .ormqueuemixin import OrmQueueMixin


class FileopsQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the FileopsQueue table.
    Note 'filename' is only here for compatibility with the ormqueuemixin
    functions and to facilitate human-readable error reports -
    request and response are the real data payload values here.
    """
    __tablename__ = 'fileopsqueue'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    added = Column(DateTime)
    after = Column(DateTime)
    sortkey = Column(DateTime, index=True)
    error = Column(Text)
    request = Column(Text)
    response = Column(Text)

    def __init__(self, request, after=None):
        """
        Create a fileops queue instance with the given request

        Parameters
        ----------
        request : str
            fileops request
        after: datetime.datetime
            Do not process this entry until after this timestamp.
        """

        self.request = request
        self.response = None
        self.filename = None
        self.added = datetime.datetime.utcnow()
        self.inprogress = False
        self.after = after if after is not None else self.added
        self.fail_dt = self.fail_dt_false  # See note in OrmQueueMixin
        self.sortkey = self.added

    def __repr__(self):
        """
        Build a string representation of this record
        """
        return f"<FileopsQueue('{self.id}', '{self.request}')>"
