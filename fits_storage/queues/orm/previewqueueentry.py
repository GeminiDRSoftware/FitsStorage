from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, Text, Boolean, DateTime

from fits_storage.core.orm import Base
from .ormqueuemixin import OrmQueueMixin
from fits_storage.core.orm.diskfile import DiskFile


class PreviewQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the previewqueue table. 
    This forms a queue of files to generate previews for.

    """
    __tablename__ = 'previewqueue'
    __table_args__ = (
        UniqueConstraint('diskfile_id', 'inprogress', 'fail_dt'),
    )

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey(DiskFile.id), nullable=False,
                         unique=True, index=True)
    filename = Column(Text)
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    force = Column(Boolean)
    sortkey = Column(Text, index=True)
    error = Column(Text)

    def __init__(self, diskfile, force=False):
        self.diskfile_id = diskfile.id
        self.filename = diskfile.filename
        self.sortkey = self.sortkey_from_filename(diskfile.filename)
        self.inprogress = False
        self.fail_dt = self.fail_dt_false  # See note in OrmQueueMixin
        self.force = force
