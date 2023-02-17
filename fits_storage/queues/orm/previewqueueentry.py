from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy import desc

from fits_storage.core import Base
from fits_storage.core.diskfile import DiskFile
from ..utils.queue import sortkey_for_filename


class PreviewQueueEntry(Base):
    """
    This is the ORM object for the previewqueue table. 
    This forms a queue of files to generate previews for.

    """
    __tablename__ = 'previewqueue'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey(DiskFile.id), nullable=False, unique=True, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    force = Column(Boolean)
    sortkey = Column(Text, index=True)

    error_name = 'PREVIEW'

    def __init__(self, diskfile, force=False):
        self.diskfile_id = diskfile.id
        self.sortkey = sortkey_for_filename(diskfile.filename)
        self.inprogress = False
        self.failed = False
        self.force = force

    @staticmethod
    def find_not_in_progress(session):
        return session.query(PreviewQueueEntry)\
                    .filter(PreviewQueueEntry.inprogress == False)\
                    .filter(PreviewQueueEntry.failed == False)\
                    .order_by(desc(PreviewQueueEntry.sortkey))

    @staticmethod
    def rebuild(session, element):
        # Dummy method; no need for this with the preview queue
        pass
