"""
This is the ingesqueue ORM class
"""
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
    sortkey = Column(Text, index=True)

    def __init__(self, filename, path):
        self.filename = filename
        self.path = path
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.force_md5 = False
        self.force = False
        self.after = self.added

        # Sortkey is used to sort the order in which we de-spool the queue.
        # If we use the filename, we end up doing all the N files before or
        # after all the S files, which is not what we want.
        # We just trim the first letter off the filename, so ignore the N or S
        # and essentially sort by date and frame number.
        self.sortkey = filename[1:]

    def __repr__(self):
        return "<IngestQueue('%s', '%s')>" % (self.id, self.filename)

