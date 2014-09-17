
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Boolean
from sqlalchemy.orm import relation

from orm.usagelog import UsageLog
from orm.diskfile import DiskFile

from . import Base

class FileDownloadLog(Base):
    """
    This is the ORM class for the filedownload log table
    """
    __tablename__ = 'filedownloadlog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey('usagelog.id'), nullable=False, index=True)
    usagelog = relation(UsageLog, order_by=id)

    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile = relation(DiskFile, order_by=id)

    ut_datetime = Column(DateTime(timezone=False), index=True)
    released = Column(Boolean)
    pi_access = Column(Boolean)
    staff_access = Column(Boolean)
    magic_access = Column(Boolean)
    eng_access = Column(Boolean)
    canhaveit = Column(Boolean)

    notes = Column(Text)

    def __init__(self, usagelog):
        """
        Create an initial FileDownloadLog instance from a UsageLog instance
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
