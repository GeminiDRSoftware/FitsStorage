import datetime
from sqlalchemy import desc, func
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from . import Base

# ------------------------------------------------------------------------------
class ExportQueue(Base):
    """
    This is the ORM object for the ExportQueue table.

    """
    __tablename__ = 'exportqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed', 'destiation'),
        UniqueConstraint('filename', 'path', 'destination'),
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
        """
        Create an :class:`~ExportQueue` record

        Parameters
        ----------
        filename : str
            Name of the file to export
        path : str
            Path of the file within the `storage_root`
        destination : str
            URL of the server to export to
        """
        self.filename = filename
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.failed = False

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
            SQL Alchemy session to query against
        """
        # The query that we're performing here is equivalent to
        #
        # WITH inprogress_filenames AS (
        #   SELECT filename FROM exportqueue
        #                   WHERE failed = false AND inprogress = True
        # )
        # SELECT id FROM exportqueue
        #          WHERE inprogress = false AND failed = false
        #          AND filename not in inprogress_filenames
        #          ORDER BY filename DESC

        inprogress_filenames = (
            session.query(ExportQueue.filename)
                .filter(ExportQueue.failed == False)
                .filter(ExportQueue.inprogress == True)
                .subquery()
        )

        return (
            session.query(ExportQueue)
                .filter(ExportQueue.inprogress == False)
                .filter(ExportQueue.failed == False)
                .filter(~ExportQueue.filename.in_(inprogress_filenames))
                .order_by(desc(ExportQueue.filename))
        )

# TODO seems to be out of use, removing for now
#     @staticmethod
#     def rebuild(session, element):
#         session.query(ExportQueue)\
#             .filter(ExportQueue.inprogress == False)\
#             .filter(ExportQueue.filename == element.filename)\
#             .delete()

    def __repr__(self):
        """
        Make a string representation of the queue item.

        Returns
        -------
        str : String representation of this record
        """
        return "<ExportQueue('%s', '%s')>" % (self.id, self.filename)

