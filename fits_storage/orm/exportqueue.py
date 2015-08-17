"""
This is the ExportQueue ORM class
"""
import datetime
from sqlalchemy import Column
from sqlalchemy import Integer, Boolean, Text, DateTime

from . import Base

class ExportQueue(Base):
    """
    This is the ORM object for the ExportQueue table
    """
    __tablename__ = 'exportqueue'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    destination = Column(Text, nullable=False, unique=False, index=True)
    inprogress = Column(Boolean, index=True)
    added = Column(DateTime)
    lastfailed = Column(DateTime)

    def __init__(self, filename, path, destination):
        self.filename = filename
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.now()
        self.inprogress = False

    @staticmethod
    def find_not_in_progress(session):
        return session.query(ExportQueue)\
                    .filter(ExportQueue.inprogress == False)\
                    .order_by(desc(ExportQueue.filename))

    @staticmethod
    def rebuild(session, element):
        session.query(ExportQueue)\
            .filter(ExportQueue.inprogress == False)\
            .filter(ExportQueue.filename == element.filename)\
            .delete()

    def __repr__(self):
        return "<ExportQueue('%s', '%s')>" % (self.id, self.filename)

