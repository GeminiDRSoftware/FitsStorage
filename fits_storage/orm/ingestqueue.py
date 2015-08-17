"""
This is the ingesqueue ORM class
"""
import datetime
from sqlalchemy import Column
from sqlalchemy import Integer, Boolean, Text, DateTime
from sqlalchemy import desc

from . import Base
from ..utils.queue import sortkey_for_filename

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
        self.sortkey = sortkey_for_filename(filename)

    @staticmethod
    def find_not_in_progress(session):
        return session.query(IngestQueue)\
                    .filter(IngestQueue.inprogress == False)\
                    .filter(IngestQueue.after < datetime.datetime.now())\
                    .order_by(desc(IngestQueue.sortkey))

    @staticmethod
    def rebuild(session, element):
        session.query(IngestQueue)\
            .filter(IngestQueue.inprogress == False)\
            .filter(IngestQueue.filename == element.filename)\
            .delete()

    def __repr__(self):
        return "<IngestQueue('%s', '%s')>" % (self.id, self.filename)

