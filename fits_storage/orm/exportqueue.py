"""
This is the ExportQueue ORM class
"""
import datetime
from sqlalchemy import desc, func
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from . import Base

class ExportQueue(Base):
    """
    This is the ORM object for the ExportQueue table
    """
    __tablename__ = 'exportqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    destination = Column(Text, nullable=False, unique=False, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    added = Column(DateTime)
    lastfailed = Column(DateTime)

    error_name = 'EXPORT'

    def __init__(self, filename, path, destination):
        self.filename = filename
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.failed = False

    @staticmethod
    def find_not_in_progress(session):
        """
        Returns a query that will find the elements in the queue that are not in progress,
        and that have no duplicates, meaning that there are not two entries where one of them
        is being processed (it's ok if there's a failed one...)
        """
        # The query that we're performing here is equivalent to
        #
        # WITH unique_filenames AS (
        #   SELECT filename FROM exportqueue
        #                   WHERE failed = false
        #                   GROUP BY filename HAVING COUNT(1) == 1
        # )
        # SELECT * FROM exportqueue AS eq JOIN unique_filenames AS uf ON eq.filename = uf.filename
        #          WHERE inprogress = false AND failed = false
        #          ORDER BY filename DESC
        unique_filenames = (
            session.query(ExportQueue.filename)
                .filter(ExportQueue.failed == False)
                .group_by(ExportQueue.filename)
                .having(func.count(1) == 1)
                .cte("unique_filenames")
        )
        return (
            session.query(ExportQueue)
                .join(unique_filenames, ExportQueue.filename == unique_filenames.c.filename)
                .filter(ExportQueue.inprogress == False)
                .filter(ExportQueue.failed == False)
                .order_by(desc(ExportQueue.filename))
        )

    @staticmethod
    def rebuild(session, element):
        session.query(ExportQueue)\
            .filter(ExportQueue.inprogress == False)\
            .filter(ExportQueue.filename == element.filename)\
            .delete()

    def __repr__(self):
        return "<ExportQueue('%s', '%s')>" % (self.id, self.filename)

