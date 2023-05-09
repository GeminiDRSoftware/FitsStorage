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
    Note 'filename' can be None in this queue, if present it will be used
    to prevent race conditions on the same filename with multiple queue service
    processes, but request and response are the real data payload values here.

    response_required == False means this queue works asynchronously like the
    other queues - the service_fileops_queue process will delete queue entries
    after processing them. response_required == True means that the queue
    code will not delete queue entries, rather it will leave them as inprogress
    and with the response set for the caller (ie the thing that added the
    entry to the queue) to read the response and then delete the queue entry.
    """
    __tablename__ = 'fileopsqueue'

    id = Column(Integer, primary_key=True)
    filename = Column(Text)
    inprogress = Column(Boolean, index=True)
    response_required = Column(Boolean)
    fail_dt = Column(DateTime, index=True)
    added = Column(DateTime)
    after = Column(DateTime)
    sortkey = Column(DateTime, index=True)
    error = Column(Text)
    request = Column(Text, nullable=False)
    response = Column(Text)

    def __init__(self, request, filename=None, after=None,
                 response_required=False):
        """
        Create a fileops queue instance with the given request

        Parameters
        ----------
        request : str
            fileops request
        after: datetime.datetime
            Do not process this entry until after this timestamp.
        filename: str
            Optional filename for this queue entry. Used to avoid race
            conditions for file-based operations.
        response_required: bool
            If True, we should leave the queue entry in place for the caller
            (that added the queue entry) to read the response out of it and
            then delete it. If False, delete the queue entry after processing
            it, like the other queues do.
        """

        self.request = request
        self.response = None
        self.filename = None
        self.added = datetime.datetime.utcnow()
        self.inprogress = False
        self.response_required = response_required
        self.after = after if after is not None else self.added
        self.fail_dt = self.fail_dt_false  # See note in OrmQueueMixin
        self.sortkey = self.added

    def __repr__(self):
        """
        Build a string representation of this record
        """
        return f"<FileopsQueue('{self.id}', '{self.request}')>"
