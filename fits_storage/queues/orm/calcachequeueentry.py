import datetime
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, Boolean, DateTime, Text

from fits_storage.core.orm import Base
from .ormqueuemixin import OrmQueueMixin
from fits_storage.core.orm.header import Header


class CalCacheQueueEntry(OrmQueueMixin, Base):
    """
    This is the ORM object for the CalCacheQueue table.

    The calcachequeue holds a list of data that we want to refresh
    our cached calibration lookups for.
    """
    __tablename__ = 'calcachequeue'
    __table_args__ = (
        UniqueConstraint('obs_hid', 'inprogress', 'fail_dt'),
    )

    id = Column(Integer, primary_key=True)
    obs_hid = Column(Integer, ForeignKey(Header.id), nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    fail_dt = Column(DateTime, index=True)
    added = Column(DateTime)
    sortkey = Column(Text, index=True);
    filename = Column(Text)
    error = Column(Text)


    def __init__(self, obs_hid, filename):
        """
        Create a :class:`~CalCacheQueueEntry` record for the given data by ID.

        Parameters
        ----------
        obs_hid : int
            ID of the Header for the data to build the calibration cache for
        filename : str
            filename related to this entry (used for error reporting and to
            form the sortkey to prioritize the queue despooling)
        """
        self.obs_hid = obs_hid
        self.inprogress = False
        self.sortkey = self.sortkey_from_filename()
        self.added = datetime.datetime.utcnow()
        self.fail_dt = datetime.datetime.max # See Note in OrmQueueMixin
        self.filename = filename
