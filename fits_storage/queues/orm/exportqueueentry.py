import datetime
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from fits_storage.core import Base
from .ormqueuemixin import OrmQueueMixin

class ExportQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the ExportQueue table.

    """
    __tablename__ = 'exportqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed', 'destination'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    path = Column(Text)
    destination = Column(Text, nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(DateTime)
    sortkey = Column(Text, index=True)
    added = Column(DateTime)
    after = Column(DateTime)
    error = Column(Text)
    md5_before_header_update = Column(Text)
    md5_after_header_update = Column(Text)
    header_update = Column(Text)

    def __init__(self, filename, path, destination):
        """
        Create an :class:`~ExportQueueEntry` record

        Parameters
        ----------
        filename : str
            Name of the file to export
        path : str
            Path of the file within the `storage_root`
        destination : str
            URL of the server to export to
        """
        self.filename = filename
        self.sortkey = self.sortkey_from_filename()
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.utcnow()
        self.after = self.added
        self.inprogress = False
        self.failed = datetime.datetime.max # See Note in OrmQueueMixin

        # TODO: A more refined way to prioritize archive exports
        prepend = 'z' if 'archive' in destination else 'a'
        self.sortkey = prepend + self.sortkey

    def __repr__(self):
        """
        Make a string representation of the queue item.

        Returns
        -------
        str : String representation of this record
        """
        return f"<ExportQueueEntry({self.id}, {self.filename}, " \
               f"{self.destination})>"