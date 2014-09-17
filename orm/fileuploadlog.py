
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Boolean
from sqlalchemy.orm import relation

from orm.usagelog import UsageLog
from orm.diskfile import DiskFile

from . import Base

class FileUploadLog(Base):
    """
    This is the ORM class for the fileupload log table
    """
    __tablename__ = 'fileuploadlog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey('usagelog.id'), nullable=False, index=True)
    usagelog = relation(UsageLog, order_by=id)

    ut_transfer_start = Column(DateTime(timezone=False), index=True)
    ut_transfer_complete = Column(DateTime(timezone=False))

    filename = Column(Text, index=True)
    size = Column(Integer)
    md5 = Column(Text)
    processed_cal = Column(Boolean)

    invoke_status = Column(Integer)
    invoke_pid = Column(Integer)

    destination = Column(Text)
    s3_ut_start = Column(DateTime(timezone=False))
    s3_ut_end = Column(DateTime(timezone=False))
    s3_ok = Column(Boolean)
    file_ok = Column(Boolean)

    ingestqueue_id = Column(Integer)

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
