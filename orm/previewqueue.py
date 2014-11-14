from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean

from . import Base

class PreviewQueue(Base):
    """
    This is the ORM object for the previewqueue table
    This forms a queue of files to generate previews for
    """
    __tablename__ = 'previewqueue'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    sortkey = Column(Text, index=True)

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id
        self.sortkey = diskfile.filename[1:]
        self.inprogress = False
