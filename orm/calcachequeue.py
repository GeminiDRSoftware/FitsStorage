"""
This is the calcachequeue ORM class
"""
import datetime
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Boolean, Text, DateTime

from . import Base

class CalCacheQueue(Base):
    """
    This is the ORM object for the CalCacheQueue table
    """
    __tablename__ = 'calcachequeue'

    id = Column(Integer, primary_key=True)
    obs_hid = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    ut_datetime = Column(DateTime)
    sortkey = Column(DateTime(timezone=False), index=True)

    def __init__(self, obs_hid, sortkey=None):
        self.obs_hid = obs_hid
        self.inprogress = False
        self.sortkey = sortkey
        self.ut_datetime = datetime.datetime.utcnow()
