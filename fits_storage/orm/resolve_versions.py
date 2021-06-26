from sqlalchemy import Column
from sqlalchemy import Integer, Text, Boolean
import os

from gemini_obs_db import Base
from ..utils.hashes import md5sum_size_fp, md5sum_size_bz2


class Version(Base):
    """
    This is the ORM class for the versions table. This is not part of the Fits
    Storage system per se. It is used by the resolve_versions.py script that we're
    using to resolve the conflicts of multiple file versions for the new archive.

    """
    __tablename__ = 'versions'

    doc1 = "If this attribute is True, we're 100% sure of the choice."
    doc2 = "If this attribute is True, resort to lastmod to sort this out."
    
    id = Column(Integer, primary_key=True)
    filename  = Column(Text, nullable=False, index=True)
    fullpath  = Column(Text)
    data_md5  = Column(Text)
    data_size = Column(Integer)
    unable    = Column(Boolean)
    score     = Column(Integer, default = -1)
    accepted  = Column(Boolean)
    is_clear  = Column(Boolean, doc = doc1)
    used_date = Column(Boolean, doc = doc2)

    def __init__(self, filename, fullpath):
        """
        Create a version record for the given filename and full path

        Parameters
        ----------
        filename : str
            Name of the file
        fullpath : str
            Full path of the file
        """
        self.filename = filename
        self.fullpath = fullpath
        self.unable = False

    def __repr__(self):
        """
        Return a string representation of this :class:`~Version` record

        Returns
        -------
        str : string representation
        """
        return "<Version({fn}, {md5}, score={sc}, acc={acc}, un={un})>".format(
                    fn =  self.filename,
                    md5 = self.data_md5,
                    acc = self.accepted,
                    un  = self.unable,
                    sc  = self.score)

    def calc_md5(self, fobj = None):
        """
        Calculate the MD% checksum for the data (uncompressed)

        Parameters
        ----------
        fobj : file-like object
            File to calculate the md5 checksum from, if provided

        Returns
        -------
        str, int : MD5 checksum and size of the uncompressed data
        """
        if fobj is not None:
            (md5, size) = md5sum_size_fp(fobj)
        else:
            (md5, size) = md5sum_size_bz2(self.fullpath)
        self.data_md5 = md5
        self.data_size = size

    def moveto(self, destdir):
        """
        Move file to the target folder

        Parameters
        ----------
        destdir : str
            Destination folder
        """
        dest = os.path.join(destdir, self.filename)
        os.rename(self.fullpath, dest)
