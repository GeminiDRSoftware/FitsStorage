from sqlalchemy import Column
from sqlalchemy import Integer, Text

from . import Base

class Authentication(Base):
    """
    This is the ORM class for file access authentication, done on a program ID basis.
    """
    __tablename__ = 'authentication'

    id = Column(Integer, primary_key=True)
    program_id = Column(Text, unique=True)
    program_key = Column(Text)
