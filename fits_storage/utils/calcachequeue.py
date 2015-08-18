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

from ..orm.header import Header
from ..orm.calcache import CalCache
from ..orm.calcachequeue import CalCacheQueue

from . import queue

from ..cal import get_cal_object
from ..cal.associate_calibrations import associate_cals

pop_calcachequeue = functools.partial(queue.pop_queue, CalCacheQueue)
calcachequeue_length = functools.partial(queue.queue_length, CalCacheQueue)

def cache_associations(session, obs_hid):
    """
    Do the calibration association and insert the associations into the calcache table.
    Remove any old associations that this replaces
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
            .delete()

        # Get the associations for this caltype
        cal_headers = associate_cals(session, [header], caltype=caltype)
        for rank, cal_header in enumerate(cal_headers):
            cc = CalCache(obs_hid, cal_header.id, caltype, rank)
            session.add(cc)
        session.commit()
