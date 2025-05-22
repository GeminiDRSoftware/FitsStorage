from sqlalchemy import Column, ForeignKey
from sqlalchemy import BigInteger, Integer, Text, DateTime, Boolean
from sqlalchemy.orm import relationship

import datetime

from fits_storage.core.orm import Base
from .usagelog import UsageLog

class FileDownloadLog(Base):
    """
    This is the ORM class for the filedownload log table
    """
    __tablename__ = 'filedownloadlog'

    id = Column(Integer, primary_key=True)
    usagelog_id = Column(BigInteger,ForeignKey(UsageLog.id), nullable=False, index=True)
    usagelog = relationship(UsageLog, order_by=id)

    # Don't reference the diskfile_id here - we want to be able to preserve
    # data over database rebuild. Reference by filename etc instead.
    diskfile_filename = Column(Text, index=True)
    diskfile_file_md5 = Column(Text)
    diskfile_file_size = Column(BigInteger)

    ut_datetime = Column(DateTime(timezone=False), index=True)
    released = Column(Boolean)
    pi_access = Column(Boolean)
    staff_access = Column(Boolean)
    magic_access = Column(Boolean)
    eng_access = Column(Boolean)
    inst_team_access = Column(Boolean)
    canhaveit = Column(Boolean)

    notes = Column(Text)

    def __init__(self, usagelog):
        """
        Create an initial FileDownloadLog instance from a UsageLog instance.

        Parameters
        ----------
        usagelog : :class:`~usagelog.Usagelog`
            The corresponding :class:`~usagelog.Usagelog` record
        """
        self.usagelog_id = usagelog.id
        self.ut_datetime = datetime.datetime.utcnow()

    def add_note(self, note):
        """
        Add a note to this log entry.

        Parameters
        ----------
        note : str
            Notes to add to this record
        """

        if self.notes is None:
            self.notes = note
        else:
            self.notes += "\n" + note
