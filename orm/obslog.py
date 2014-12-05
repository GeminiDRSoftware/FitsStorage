from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Date
from sqlalchemy.orm import relation

import datetime

from . import Base
from orm.diskfile import DiskFile

class Obslog(Base):
    """
    This is the ORM class for the obslog table
    """
    __tablename__ = 'obslog'

    
    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile = relation(DiskFile, order_by=id)
    program_id = Column(Text, index=True)
    date = Column(Date, index=True)

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id

    def __repr__(self):
        return "<Obslog('%s', '%s', '%s')>" % (self.id, self.program_id, self.date)
