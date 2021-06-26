"""
This is the queue_error ORM class.

"""
import datetime
from sqlalchemy import Column
from sqlalchemy import Integer, Text, DateTime, Enum

from gemini_obs_db import Base

CALCACHEQUEUE = 'CALCACHE'
EXPORTQUEUE   = 'EXPORT'
INGESTQUEUE   = 'INGEST'
PREVIEWQUEUE =  'PREVIEW'

queues = (INGESTQUEUE, PREVIEWQUEUE, CALCACHEQUEUE, EXPORTQUEUE)

QUEUE_ENUM = Enum(*queues, name='queue')


class QueueError(Base):
    """
    This is the ORM object for the QueueError table.

    """
    __tablename__ = 'queue_error'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    added = Column(DateTime)
    queue = Column(QUEUE_ENUM, unique=False, index=True)
    error = Column(Text)

    def __init__(self, filename, path, queue, error=None):
        """
        Create a record of a queue error, something went wrong processing a queue.

        Parameters
        ----------
        filename : str
            Name of file involved in the queue action that errored out
        path : str
            Path within `storage_root` of the file
        queue : str
            Which queue had the issue (one of :field:`~CALCACHEQUEUE`, :field:`~EXPORTQUEUE`,
            :field:`~INGESTQUEUE`, :field:`~PREVIEWQUEUE`
        error : str
            Details on the error that occured
        """
        self.filename = filename
        self.path = path
        self.added = datetime.datetime.now()
        self.queue = queue
        self.error = error

    @classmethod
    def get_errors_from(cls, session, queue_class):
        """
        Get the error records for a given queue (by it's class)

        Parameters
        ----------
        session : :class:`~sqlalchemy.orm.session.Session`
            SQLAlchemy session to query
        queue_class : one of :class:`~CalCacheQueue`, :class:`~ExportQueue`, :class:`~IngestQueue`,
            or :class:`~PreviewQueue`

        Returns
        -------
        :class:`sqlalchemy.org.query.Query`
            query object for SQLAlchemy for errors with the Queue's defined `error_name` as the `queue` field.
        """
        return session.query(cls).filter(cls.queue == queue_class.error_name)
