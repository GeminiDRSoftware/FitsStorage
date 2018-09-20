from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime, Boolean, BigInteger

from sqlalchemy.orm import relation, relationship

from . import Base

# ------------------------------------------------------------------------------
class Tape(Base):
    """
    This is the ORM object for the Tape table.
    Each row in this table represents a data tape.

    """
    __tablename__ = 'tape'

    id = Column(Integer, primary_key=True)
    label = Column(Text, nullable=False, index=True)
    firstwrite = Column(DateTime(timezone=False))
    lastwrite = Column(DateTime(timezone=False))
    lastverified = Column(DateTime(timezone=False))
    location = Column(Text)
    lastmoved = Column(DateTime(timezone=False))
    active = Column(Boolean, index=True)
    full = Column(Boolean, index=True)
    set = Column(Integer, index=True)
    fate = Column(Text)

    tapewrites = relationship('TapeWrite')

    def __init__(self, label):
        self.label = label
        self.active = True
        self.full = False
        self.set = 0

class TapeWrite(Base):
    """
    This is the ORM object for the TapeWrite table.
    Each row in this table represents a tape writing session.

    """

    __tablename__ = 'tapewrite'

    id = Column(Integer, primary_key=True)
    tape_id = Column(Integer, ForeignKey('tape.id'), nullable=False, index=True)
    tape = relation(Tape, order_by=id)
    filenum = Column(Integer, index=True)
    startdate = Column(DateTime(timezone=False))
    enddate = Column(DateTime(timezone=False))
    suceeded = Column(Boolean, index=True)
    size = Column(BigInteger)
    beforestatus = Column(Text)
    afterstatus = Column(Text)
    hostname = Column(Text)
    tapedrive = Column(Text)
    notes = Column(Text)

    tapefiles = relationship('TapeFile')


class TapeFile(Base):
    """
    This is the ORM object for the TapeFile table.

    """
    __tablename__ = 'tapefile'

    id = Column(Integer, primary_key=True)
    tapewrite_id = Column(Integer,ForeignKey('tapewrite.id'),nullable=False,index=True)
    tapewrite = relation(TapeWrite, order_by=id)
    filename = Column(Text, index=True)
    size = Column(Integer, index=True)
    md5 = Column(Text, index=True)
    compressed = Column(Boolean)
    data_size = Column(Integer)
    data_md5 = Column(Text)
    lastmod = Column(DateTime(timezone=True), index=True)

class TapeRead(Base):
    """
    This is the ORM object for the TapeRead table.

    """
    __tablename__ = 'taperead'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, index=True)
    md5 = Column(Text, index=True)
    requester = Column(Text)


