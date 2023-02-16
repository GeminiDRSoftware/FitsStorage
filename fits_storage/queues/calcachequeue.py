"""
This is the calcachequeue ORM class

"""
import datetime
from sqlalchemy import Column, ForeignKey, Text
from sqlalchemy import Integer, Boolean, DateTime
from sqlalchemy import desc

from fits_storage.core import Base
from fits_storage.core.header import Header


class CalCacheQueue(Base):
    """
    This is the ORM object for the CalCacheQueue table.

    The calcachequeue holds a list of data that we want to refresh
    our cached calibration lookups for.
    """
    __tablename__ = 'calcachequeue'

    id = Column(Integer, primary_key=True)
    obs_hid = Column(Integer, ForeignKey(Header.id), nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    ut_datetime = Column(DateTime)
    sortkey = Column(DateTime(timezone=False), index=True)
    filename = Column(Text)

    error_name = 'CALCACHE'

    @staticmethod
    def find_not_in_progress(session):
        """
        Find :class:`~CalCacheQueue` record that are not currently in progress.

        This queries for any records that are not in progress or failed, ordering them by the
        sortkey.

        Returns
        -------

        :class:`sqlalchemy.orm.query.Query` SQL Alchemy query object
        """
        return session.query(CalCacheQueue)\
                    .filter(CalCacheQueue.inprogress == False)\
                    .filter(CalCacheQueue.failed == False)\
                    .order_by(desc(CalCacheQueue.sortkey))

    # TODO remove tjhis
    # It looks like the rebuild logic is no longer used, I think we can delete these
    # @staticmethod
    # def rebuild(session, element):
    #     # This method seems not to be used anywhere.
    #     session.query(CalCacheQueue)\
    #             .filter(CalCacheQueue.inprogress == False)\
    #             .filter(CalCacheQueue.obs_hid == element.obs_hid)\
    #             .delete()

    def __init__(self, obs_hid, filename, sortkey=None):
        """
        Create a :class:`~CalCacheQueue` record for the given data by ID.

        Parameters
        ----------
        obs_hid : int
            ID of the Header for the data to build the calibration cache for
        filename : str
            filename related to this entry (used for error reporting)
        sortkey : datetime
            A sortable entry to provide a mechanism to front-load some data when we want to in the queue
        """
        self.obs_hid = obs_hid
        self.inprogress = False
        self.sortkey = sortkey
        self.ut_datetime = datetime.datetime.utcnow()
        self.failed = False
        self.filename = filename
