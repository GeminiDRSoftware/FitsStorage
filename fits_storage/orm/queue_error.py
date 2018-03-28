#
#                                                                    FitsStorage
#
#                                                             Gemini Observatory
#                                                  fits_store.orm.queue_error.py
# ------------------------------------------------------------------------------
__version__      = '0.99 beta'
# ------------------------------------------------------------------------------
"""
This is the queue_error ORM class.

"""
import datetime
from sqlalchemy import Column
from sqlalchemy import Integer, Text, DateTime, Enum

from . import Base

CALCACHEQUEUE = 'CALCACHE'
EXPORTQUEUE   = 'EXPORT'
INGESTQUEUE   = 'INGEST'
PREVIEWQUEUE =  'PREVIEW'

queues = (INGESTQUEUE, PREVIEWQUEUE, CALCACHEQUEUE, EXPORTQUEUE)

QUEUE_ENUM = Enum(*queues, name='queue')

# ------------------------------------------------------------------------------
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

    def __init__(self, filename, path, queue, error = None):
        self.filename = filename
        self.path = path
        self.added = datetime.datetime.now()
        self.queue = queue
        self.error = error

    @classmethod
    def get_errors_from(cls, session, queue_class):
        return session.query(cls).filter(cls.queue == queue_class.error_name)
