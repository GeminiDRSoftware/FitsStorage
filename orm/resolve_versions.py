from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean

from utils.hashes import md5sum_size_bz2

from . import Base

class Version(Base):
    """
    This is the ORM class for the versions table. This is not part of the Fits Storage system per se
    It is used by the resolve_versions.py script that we're using to resolve the conflicts of multiple
    file versions for the new archive.
    """
    __tablename__ = 'versions'

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, index=True)
    fullpath = Column(Text)
    data_md5 = Column(Text)
    data_size = Column(Integer)
    unable = Column(Boolean)

    def __init__(self, filename, fullpath):
        self.filename = filename
        self.fullpath = fullpath
        self.unable = False

    def calc_md5(self):
        (md5, size) = md5sum_size_bz2(self.fullpath)
        self.data_md5 = md5
        self.data_size = size

    def moveto(self, destdir):
        dest = os.path.join(destdir, self.filename)
        os.rename(self.fullpath, dest)
