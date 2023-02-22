import datetime

from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, Text, Boolean, DateTime

from fits_storage.core import Base
from .ormqueuemixin import OrmQueueMixin
from fits_storage.core.diskfile import DiskFile

class PreviewQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the previewqueue table. 
    This forms a queue of files to generate previews for.

    """
    __tablename__ = 'previewqueue'
    __table_args__ = (
        UniqueConstraint('diskfile_id', 'inprogress', 'failed'),
    )

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey(DiskFile.id), nullable=False,
                         unique=True, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(DateTime)
    force = Column(Boolean)
    sortkey = Column(Text, index=True)
    error = Column(Text)

    def __init__(self, diskfile, force=False):
        self.diskfile_id = diskfile.id
        self.sortkey = self.sortkey_from_filename(diskfile.filename)
        self.inprogress = False
        self.failed = datetime.datetime.max # See note in OrmQueueMixin
        self.force = force
