from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, String
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from . import Base

class ProgramPublication(Base):
    """
    Association object supporting the M:N relationship between programs
    and publications.
    """
    __tablename__ = 'programpublication'

    prog_id = Column(Integer, ForeignKey('program.id'), primary_key = True)
    pub_id = Column(Integer, ForeignKey('publication.id'), primary_key = True)
    program_text_id = Column(Text, nullable=False, index=True)
    bibcode = Column(String(20), nullable=False, index=True)
    publication = relationship('Publication', back_populates='programs')
    program = relationship('Program')
