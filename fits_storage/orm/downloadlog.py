from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Boolean
from sqlalchemy.orm import relation

from gemini_obs_db import Base
from .usagelog import UsageLog


class DownloadLog(Base):
    """
    This is the ORM class for the download log table.

    """
    __tablename__ = 'downloadlog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey('usagelog.id'), nullable=False,
                         index=True)
    usagelog = relation(UsageLog, order_by=id)

    selection = Column(Text)
    numresults = Column(Integer)
    sending_files = Column(Boolean)
    numdenied = Column(Integer)

    query_started = Column(DateTime(timezone=False))
    query_completed = Column(DateTime(timezone=False))
    download_completed = Column(DateTime(timezone=False))

    notes = Column(Text)

    def __init__(self, usagelog):
        """
        Create an initial QueryLog instance from a UsageLog instance.

        Parameters
        ----------
        useagelog : :class:`~usagelog.UsageLog`
        """
        self.usagelog_id = usagelog.id

    def add_note(self, note):
        """
        Add a note to this log entry.

        Parameters
        ----------
        note : str
            Notes to append on the download log entry
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note
