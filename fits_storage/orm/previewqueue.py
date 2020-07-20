from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy import desc

from . import Base
from ..utils.queue import sortkey_for_filename


class PreviewQueue(Base):
    """
    This is the ORM object for the previewqueue table. 
    This forms a queue of files to generate previews for.

    """
    __tablename__ = 'previewqueue'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, unique=True, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    sortkey = Column(Text, index=True)

    error_name = 'PREVIEW'

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id
        self.sortkey = sortkey_for_filename(diskfile.filename)
        self.inprogress = False
        self.failed = False

    @staticmethod
    def find_not_in_progress(session):
        return session.query(PreviewQueue)\
                    .filter(PreviewQueue.inprogress == False)\
                    .filter(PreviewQueue.failed == False)\
                    .order_by(desc(PreviewQueue.sortkey))

    @staticmethod
    def rebuild(session, element):
        # Dummy method; no need for this with the preview queue
        pass
