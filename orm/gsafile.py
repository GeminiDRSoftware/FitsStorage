from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation

from . import Base

from orm.file import File

class GsaFile(Base):
    """
    This is the ORM object for the GsaFile.
    Contains md5 values polled from the GSA to allow us
    to verify if we have the same file version as the GSA
    """
    __tablename__ = 'gsafile'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
    file = relation(File, order_by=id)
    md5 = Column(Text)
    ingestdate = Column(DateTime(timezone=True))
    lastpoll = Column(DateTime(timezone=True), index=True)
