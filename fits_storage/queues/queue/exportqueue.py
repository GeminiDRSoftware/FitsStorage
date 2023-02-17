"""
ExportQueue housekeeping class. Note that this is not the ORM class, which is
now called ...QueueEntry as it represents an entry on the queue as opposed to
the queue itself.
"""

from .queue import Queue
from ..orm.exportqueueentry import ExportQueueEntry

class ExportQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=ExportQueueEntry, logger=logger)
