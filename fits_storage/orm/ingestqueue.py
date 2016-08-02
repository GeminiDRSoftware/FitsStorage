"""
This is the ingesqueue ORM class
"""
import datetime
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime
from sqlalchemy import desc, func

from . import Base
from ..utils.queue import sortkey_for_filename

class IngestQueue(Base):
    """
    This is the ORM object for the IngestQueue table
    """
    __tablename__ = 'ingestqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    added = Column(DateTime)
    force_md5 = Column(Boolean)
    force = Column(Boolean)
    after = Column(DateTime)
    sortkey = Column(Text, index=True)

    error_name = 'INGEST'

    def __init__(self, filename, path):
        self.filename = filename
        self.path = path
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.force_md5 = False
        self.force = False
        self.after = self.added
        self.failed = False

        # Sortkey is used to sort the order in which we de-spool the queue.
        self.sortkey = sortkey_for_filename(filename)

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
        #   SELECT filename FROM ingestqueue
        #                   WHERE failed = false
        #                   GROUP BY filename HAVING COUNT(1) == 1
        # )
        # SELECT * FROM ingestqueue AS eq JOIN unique_filenames AS uf ON eq.filename = uf.filename
        #          WHERE inprogress = false AND failed = false
        #          ORDER BY filename DESC

        unique_filenames = (
            session.query(IngestQueue.filename)
                .filter(IngestQueue.failed == False)
                .group_by(IngestQueue.filename)
                .having(func.count(1) == 1)
                .cte("unique_filenames")
        )
        return (
            session.query(IngestQueue)
                .join(unique_filenames, IngestQueue.filename == unique_filenames.c.filename)
                .filter(IngestQueue.inprogress == False)
                .filter(IngestQueue.failed == False)
                .filter(IngestQueue.after < datetime.datetime.now())\
                .order_by(desc(IngestQueue.sortkey))
        )

    @staticmethod
    def rebuild(session, element):
        session.query(IngestQueue)\
            .filter(IngestQueue.inprogress == False)\
            .filter(IngestQueue.filename == element.filename)\
            .delete()

    def __repr__(self):
        return "<IngestQueue('%s', '%s')>" % (self.id, self.filename)

