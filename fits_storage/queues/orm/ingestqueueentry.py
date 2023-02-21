"""
This is the IngestQueueEntry ORM class.

"""
import datetime

from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from fits_storage.core import Base
from .ormqueuemixin import OrmQueueMixin

class IngestQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the IngestQueue table
    """
    __tablename__ = 'ingestqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'path', 'inprogress', 'failed'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    path = Column(Text)
    inprogress = Column(Boolean, index=True)
    failed = Column(DateTime)
    added = Column(DateTime)
    force_md5 = Column(Boolean)
    force = Column(Boolean)
    after = Column(DateTime)
    sortkey = Column(Text, index=True)
    error = Column(Text)
    md5_before_header_update = Column(Text)
    md5_after_header_update = Column(Text)
    header_update = Column(Text)

    def __init__(self, filename, path, force=False, force_md5=False,
                 after=None):
        """
        Create an :class:`~orm.ingestqueue.IngestQueue` instance with the
        given filename and path

        Parameters
        ----------
        filename : str
            Name of the file to ingest
        path : str
            Path of the file within the `storage_root`
        force: bool
            Whether to force reingestion
        force_md5: bool
            Whether for force checking of the file md5sum irrespective of the
            lastmod timestamp on the filesystem in deciding whether to reingest
        after: datetime.datetime
            Do not ingest this file until after this timestamp.
        """

        self.filename = filename
        self.path = path
        self.added = datetime.datetime.utcnow()
        self.inprogress = False
        self.force_md5 = force_md5
        self.force = force
        self.after = after if after is not None else self.added
        self.failed = None
        self.sortkey = self.sortkey_from_filename()

    def __repr__(self):
        """
        Build a string representation of this :class:`~IngestQueueEntry` record

        Returns
        -------
            str : String representation of the :class:`~IngestQueueEntry` record
        """
        return f"<IngestQueue('{self.id}', '{self.filename}')>"
