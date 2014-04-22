import datetime
from sqlalchemy import Column
from sqlalchemy import Integer, Boolean, Text, DateTime

from . import Base

class IngestQueue(Base):
    """
    This is the ORM object for the IngestQueue table
    """
    __tablename__ = 'ingestqueue'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    inprogress = Column(Boolean, index=True)
    added = Column(DateTime)
    force_md5 = Column(Boolean)
    force = Column(Boolean)
    after = Column(DateTime)

    def __init__(self, filename, path):
        self.filename = filename
        self.path = path
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.force_md5 = False
        self.force = False
        self.after = self.added

    def __repr__(self):
        return "<IngestQueue('%s', '%s')>" % (self.id, self.filename)

