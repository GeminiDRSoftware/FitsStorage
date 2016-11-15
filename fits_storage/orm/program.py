from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean, Float, DateTime
from sqlalchemy.orm import relationship

from . import Base

class Program(Base):
    """
    This is the ORM class for storing program details fetched from the ODB.
    """
    __tablename__ = 'program'

    id = Column(Integer, primary_key=True)
    program_id = Column(Text, index=True)
    pi_coi_names = Column(Text, index=True)
    title = Column(Text)
    abstract = Column(Text)
    piemail = Column(Text)
    coiemail = Column(Text)
    science_band = Column(Integer)
    partners = Column(Text)
    rollover = Column(Boolean)
    too = Column(Boolean)
    completion = Column(Text)
    allocated_hours = Column(Float)
    remaining_hours = Column(Float)
    last_refreshed = Column(DateTime, index=True)
    programs = relationship("ProgramPublication", back_populates='program')

    def __init__(self, program_id):
        self.program_id = program_id

