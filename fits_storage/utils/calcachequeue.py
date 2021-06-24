"""
This module provides various utility functions to
manage and service the calcache queue
"""
import os
import datetime
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient
import functools

from gemini_obs_db.header import Header
from gemini_obs_db.calcache import CalCache
from ..orm.calcachequeue import CalCacheQueue

from . import queue

from gemini_calmgr.cal import get_cal_object
from gemini_calmgr.cal.associate_calibrations import associate_cals


class CalCacheQueueUtil(object):
    """
    Helper class for working with the queue for creating CalCache records.
    """
    def __init__(self, session, logger):
        """
        Create a :class:`~CalCacheQueueUtil`

        Parameters
        ----------
        session : :class:`sqlalchemy.orm.session.Session
            SQL Alchemy session to work in
        logger : :class:`logger.Logger`
            Logger for logging messages
        """
        self.s = session
        self.l = logger

    def length(self):
        """
        Get the current length of the queue

        Returns
        -------
        int : length of the queue, including in progress and failed entries
        """
        return queue.queue_length(CalCacheQueue, self.s)

    def pop(self, fast_rebuild=False):
        """
        Take the last entry off the queue

        Returns
        -------
        :class:`~orm.calcachequeue.CalCacheQueue` : next item on the queue
        """
        return queue.pop_queue(CalCacheQueue, self.s, self.l, fast_rebuild)

    def set_error(self, trans, exc_type, exc_value, tb):
        "Sets an error message to a transient object"
        queue.add_error(CalCacheQueue, trans, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        "Deletes a transient object"
        queue.delete_with_id(CalCacheQueue, trans.id, self.s)


def cache_associations(session, obs_hid):
    """
    Do the calibration association and insert the associations into the calcache table.
    Remove any old associations that this replaces

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session
        SQL Alchemy session to work in
    obs_hid : int
        ID of Header to perform Calibration associations on and save in cache
    """

    # Get the Header object
    header = session.query(Header).get(obs_hid)

    if None in [header.instrument, header.ut_datetime]:
        return

    # Get a cal object for it
    cal = get_cal_object(session, None, header)

    # Loop through the applicable calibration types
    for caltype in cal.applicable:
        # Blow away old associations of this caltype
        session.query(CalCache)\
            .filter(CalCache.obs_hid == header.id)\
            .filter(CalCache.caltype == caltype)\
            .delete(synchronize_session=False)

        session.commit()
        # Get the associations for this caltype
        cal_headers = associate_cals(session, [header], caltype=caltype)
        for rank, cal_header in enumerate(cal_headers):
            cc = CalCache(obs_hid, cal_header.id, caltype, rank)
            session.add(cc)
        session.commit()
