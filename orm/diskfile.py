from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relation

import os
import datetime
from utils.hashes import md5sum, md5sum_size_gz

from . import Base
from orm.file import File

from fits_storage_config import storage_root

class DiskFile(Base):
    """
    This is the ORM class for the diskfile table. A diskfile represents an instance of a file on disk.
    If the file is compressed (gzipped) we keep some metadata on the actual file as is and also on the
    decompressed data. file_md5 and file_size are those of the actual file. data_md5 and data_size correspond
    to the uncompressed data if the file is compressed, and should be the same as for file_ for uncompressed files.
    """
    __tablename__ = 'diskfile'

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
    file = relation(File, order_by=id)

    filename = Column(Text, index=True)
    path = Column(Text)
    present = Column(Boolean, index=True)
    canonical = Column(Boolean, index=True)
    file_md5 = Column(Text)
    file_size = Column(Integer)
    lastmod = Column(DateTime(timezone=True), index=True)
    entrytime = Column(DateTime(timezone=True), index=True)

    gzipped = Column(Boolean)
    data_md5 = Column(Text)
    data_size = Column(Integer)

    isfits = Column(Boolean)
    fvwarnings = Column(Integer)
    fverrors = Column(Integer)
    wmdready = Column(Boolean)

    def __init__(self, given_file, given_filename, path, gzipped=None):
        self.file_id = given_file.id
        self.filename = given_filename
        self.path = path
        self.present = True
        self.canonical = True
        self.entrytime = datetime.datetime.now()
        self.file_size = self.get_file_size()
        self.file_md5 = self.get_file_md5()
        self.lastmod = self.get_lastmod()
        if(gzipped==True or given_filename.endswith(".gz")):
            self.gzipped = True
            (u_md5, u_size) = md5sum_size_gz(self.fullpath())
            self.data_md5 = u_md5
            self.data_size = u_size
        else:
            self.gzipped = False
            self.data_md5 = self.file_md5
            self.data_size = self.file_size

    def fullpath(self):
        return os.path.join(storage_root, self.path, self.filename)

    def get_file_size(self):
        return os.path.getsize(self.fullpath())

    def exists(self):
        exists = os.access(self.fullpath(), os.F_OK | os.R_OK)
        isfile = os.path.isfile(self.fullpath())
        return (exists and isfile)

    def get_file_md5(self):
        return md5sum(self.fullpath())

    def get_lastmod(self):
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath()))

    def __repr__(self):
        return "<DiskFile('%s', '%s', '%s', '%s')>" % (self.id, self.file_id, self.filename, self.path)
