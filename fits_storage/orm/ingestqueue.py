"""
This is the ingesqueue ORM class.

"""
import datetime
import json

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
    header_fields = Column(Text)
    md5_before_header = Column(Text)
    md5_after_header = Column(Text)
    reject_new = Column(Boolean)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    added = Column(DateTime)
    force_md5 = Column(Boolean)
    force = Column(Boolean)
    after = Column(DateTime)
    sortkey = Column(Text, index=True)

    error_name = 'INGEST'

    def __init__(self, filename, path, header_fields=None, md5_before_header=None, md5_after_header=None,
                 reject_new=True):
        """
        Create an :class:`~orm.ingestqueue.IngestQueue` instance with the given filename and path

        Parameters
        ----------
        filename : str
            Name of the file to ingest
        path : str
            Path of the file within the `storage_root`
        """
        if header_fields and (not md5_before_header or not md5_after_header):
            print("IngestQueue constructor MD5(s) missing but header_fields seen, throwing ValueError")
            raise ValueError("header_fields specified but missing before and after md5 checksums")
        if header_fields:
            try:
                print("Loading header fields as json: %s" % header_fields)
                json.loads(header_fields)
            except:
                print("Invalid JSON, raising error")
                raise ValueError(f"Invalid json in passed header_fields: {header_fields}")

        print("setting other IQ values")
        self.filename = filename
        self.path = path
        self.header_fields = header_fields
        self.md5_before_header = md5_before_header
        self.md5_after_header = md5_after_header
        self.reject_new = reject_new
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.force_md5 = False
        self.force = False
        self.after = self.added
        self.failed = False

        # Sortkey is used to sort the order in which we de-spool the queue.
        print("done setting other IQ values, setting sortkey")
        self.sortkey = sortkey_for_filename(filename)
        print("done setting sortkey and done constructing IQ")

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
