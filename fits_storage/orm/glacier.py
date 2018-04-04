from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relation

from . import Base

# ------------------------------------------------------------------------------
class Glacier(Base):
    __tablename__ = 'glacier'
    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    md5 = Column(Text, nullable=False, index=True)
    # datetime the file was copied to glacier
    when_uploaded = Column(DateTime)
    # datetime we last checked this table row against the bucket listing
    last_inventory = Column(DateTime)
