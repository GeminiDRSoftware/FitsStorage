from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relation

import os
import datetime
import gzip
from utils.hashes import md5sum, md5sum_size_gz

from . import Base
from orm.file import File

from fits_storage_config import storage_root, gz_staging_area

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

    # We use this to store an uncompressed Cache of a gzipped file
    # This is not recorded in the database and is transient for the life
    # of this diskfile instance.
    uncompressed_cache_file = None

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
            # Create the unzipped cache filename and unzip to it
            try:
                if(given_filename.endswith(".gz")):
                    nongzfilename = given_filename[:-3]
                else:
                    nongzfilename = gived_filename + "_gunzipped"
                self.uncompressed_cache_file = os.path.join(gz_staging_area, nongzfilename)
                if(os.path.exists(self.uncompressed_cache_file)):
                    os.unlink(self.uncompressed_cache_file)
                in_file = gzip.GzipFile(self.fullpath(), mode='rb')
                out_file = open(self.uncompressed_cache_file, 'w')
                out_file.write(in_file.read())
                in_file.close()
                out_file.close()
            except:
                # Failed to create the unzipped cache file
                self.uncompressed_cache_file = None
     
            self.data_md5 = self.get_data_md5()
            self.data_size = self.get_data_size()
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

    def get_data_md5(self):
        if(self.gzipped == False):
            return self.file_md5()
        else:
            if(self.uncompressed_cache_file):
                return md5sum(self.uncompressed_cache_file)
            else:
                (u_md5, u_size) = md5sum_size_gz(self.fullpath())
                return u_md5

    def get_data_size(self):
        if(self.gzipped == False):
            return self.file_size()
        else:
            if(self.uncompressed_cache_file):
                return os.path.getsize(self.uncompressed_cache_file)
            else:
                (u_md5, u_size) = md5sum_size_gz(self.fullpath())
                return u_size

    def get_lastmod(self):
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath()))

    def __repr__(self):
        return "<DiskFile('%s', '%s', '%s', '%s')>" % (self.id, self.file_id, self.filename, self.path)
