import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Boolean
from sqlalchemy.orm import relation

from gemini_obs_db.db import Base
from .usagelog import UsageLog
from gemini_obs_db.orm.diskfile import DiskFile

# ------------------------------------------------------------------------------
class FileUploadLog(Base):
    """
    This is the ORM class for the fileupload log table.

    """
    __tablename__ = 'fileuploadlog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey(UsageLog.id), nullable=False, index=True)
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

        Parameters
        ----------
        usagelog : :class:`~usagelog.Usagelog`
            Corresponding usagelog entry
        """
        self.usagelog_id = usagelog.id

    def add_note(self, note):
        """
        Add a note to this log entry.

        Parameters
        ----------
        note : str
            Note to add to the notes on this record
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note


class FileUploadWrapper(object):
    def __init__(self, fileuploadlog = None):
        self.ful = fileuploadlog

    def __getattr__(self, name):
        return getattr(self.ful, name)

    def set_wrapped(self, ful):
        self.ful = ful

    def __enter__(self):
        try:
            self.ful.s3_ut_start = datetime.datetime.utcnow()
        except AttributeError:
            # The file upload log is None - act as dummy
            pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            try:
                self.ful.s3_ut_end = datetime.datetime.utcnow()
            except AttributeError:
                # The file upload log is None - act as dummy
                pass
