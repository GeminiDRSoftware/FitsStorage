from sqlalchemy import Column
from sqlalchemy import Integer, Text
#from sqlalchemy.orm import relationship

from . import Base

class File(Base):
    """
    This is the ORM class for the file table. This is highest level most abstract concept of a 'File'
    It's essentially just a unique label that other things - actual DiskFiles for example can reference.
    The 'name' column here may not be the actual filename - the definitive filename is in the diskfile table,
    when we have a compressed (gzipped) file, we trim off the .gz here.
    """
    __tablename__ = 'file'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, unique=True, index=True)

    def __init__(self, filename):
        self.name = self.trim_name(filename)

    def __repr__(self):
        return "<File('%s', '%s')>" % (self.id, self.name)

    def trim_name(self, filename):
        """
        Trim any trailing .gz off the filename
        """
        name = filename
        if filename.endswith(".gz"):
            name = filename[:-3]
        return name

