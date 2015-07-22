
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation

from . import Base
from .usagelog import UsageLog

class QueryLog(Base):
    """
    This is the ORM class for the query log table
    """
    __tablename__ = 'querylog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey('usagelog.id'), nullable=False, index=True)
    usagelog = relation(UsageLog, order_by=id)

    summarytype = Column(Text, index=True)
    selection = Column(Text)
    numresults = Column(Integer)
    numcalresults = Column(Integer)

    query_started = Column(DateTime(timezone=False))
    query_completed = Column(DateTime(timezone=False))
    cals_completed = Column(DateTime(timezone=False))
    summary_completed = Column(DateTime(timezone=False))

    notes = Column(Text)

    def __init__(self, usagelog):
        """
        Create an initial QueryLog instance from a UsageLog instance
        """
        self.usagelog_id = usagelog.id

    def add_note(self, note):
        """
        Add a note to this log entry.
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note
