"""
This is the ingesqueue ORM class.

"""
import datetime
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime
from sqlalchemy import desc, func

from gemini_obs_db.db import Base
from ..utils.queue import sortkey_for_filename


class IngestQueue(Base):
    """
    This is the ORM object for the IngestQueue table
    """
    __tablename__ = 'ingestqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed'),
        UniqueConstraint('filename', 'path'),
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
        """
        Create an :class:`~IngestQueue` instance with the given filename and path

        Parameters
        ----------
        filename : str
            Name of the file to ingest
        path : str
            Path of the file within the `storage_root`
        """
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
        Returns a query that will find the elements in the queue that are not 
        in progress, and that have no duplicates, meaning that there are not two
        entries where one of them is being processed (it's ok if there's a failed 
        one...)

        Parameters
        ----------
        session : :class:`sqlalchemy.orm.session.Session`
            SQL Alchemy session to query in
        """
        # The query that we're performing here is equivalent to
        #
        # WITH inprogress_filenames AS (
        #   SELECT filename FROM ingestqueue
        #                   WHERE failed = false AND inprogress = True
        # )
        # SELECT id FROM ingestqueue
        #          WHERE inprogress = false AND failed = false
        #          AND filename not in inprogress_filenames
        #          ORDER BY filename DESC

        inprogress_filenames = (session.query(IngestQueue.filename)
                .filter(IngestQueue.failed == False)
                .filter(IngestQueue.inprogress == True)
                .subquery()
        )

        return (
            session.query(IngestQueue)
                .filter(IngestQueue.inprogress == False)
                .filter(IngestQueue.failed == False)
                .filter(IngestQueue.after < datetime.datetime.now())
                .filter(~IngestQueue.filename.in_(inprogress_filenames))
                .order_by(desc(IngestQueue.sortkey))
        )

    # TODO this seems to be something we can get rid of
    # @staticmethod
    # def rebuild(session, element):
    #     session.query(IngestQueue)\
    #         .filter(IngestQueue.inprogress == False)\
    #         .filter(IngestQueue.filename == element.filename)\
    #         .delete()

    def __repr__(self):
        """
        Build a string representation of this :class:`~IngestQueue` record

        Returns
        -------
            str : String representation of the :class:`~IngestQueue` record
        """
        return "<IngestQueue('{}', '{}')>".format((self.id, self.filename))
