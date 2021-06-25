from sqlalchemy import Column, ForeignKey
from sqlalchemy import BigInteger, Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relation

import os
import datetime
import bz2
import re

from .provenance import Provenance, ProvenanceHistory
from .utils.hashes import md5sum, md5sum_size_bz2

from . import Base
from .file import File
from .preview import Preview

#from ..fits_storage_config import storage_root, z_staging_area
# from .. import fits_storage_config as fsc
from .db_config import storage_root, z_staging_area

_standard_filename_timestamp_re = re.compile(r'[NS](\d{4})(\d{2})(\d{2})[A-Z].*')
_igrins_filename_timestamp_re = re.compile(r'[A-Z]{4}_(\d{4})(\d{2})(\d{2})_.*')
_skycam_filename_timestamp_re = re.compile(r'img_(\d{4})(\d{2})(\d{2})_\d{2}h\d{2}m\d{2}s.fits')
_fallback_filename_timestamp_re = re.compile(r'.*(20\d{2})([0-1]\d)([0-3]\d).*')


def _determine_timestamp_from_filename(filename):
    for regex in [
        _standard_filename_timestamp_re,
        _igrins_filename_timestamp_re,
        _skycam_filename_timestamp_re,
        _fallback_filename_timestamp_re
    ]:
        m = regex.search(filename)
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            dt = datetime.datetime(year=year, month=month, day=day)
            return dt
    # Unrecognized filename format, upstream will have to do something appropriate
    return None


class DiskFile(Base):
    """
    This is the ORM class for the diskfile table. A diskfile represents an
    instance of a file on disk. If the file is compressed (with bzip2) we keep
    some metadata on the actual file as is and also on the decompressed data. 
    file_md5 and file_size are those of the actual file. data_md5 and data_size
    correspond to the uncompressed data if the file is compressed, and should be
    the same as for file_ for uncompressed files.

    """
    __tablename__ = 'diskfile'

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
    file = relation(File, order_by=id)
    previews = relation("Preview", order_by=Preview.filename)

    filename = Column(Text, index=True)
    path = Column(Text)
    present = Column(Boolean, index=True)
    canonical = Column(Boolean, index=True)
    file_md5 = Column(Text)
    file_size = Column(BigInteger)
    lastmod = Column(DateTime(timezone=True), index=True)
    entrytime = Column(DateTime(timezone=True), index=True)

    compressed = Column(Boolean)
    data_md5 = Column(Text)
    data_size = Column(BigInteger)

    isfits = Column(Boolean)
    fvwarnings = Column(Integer)
    fverrors = Column(Integer)
    mdready = Column(Boolean)

    datafile_timestamp = Column(DateTime(timezone=True), index=True)

    provenance = relation(Provenance, backref='diskfile', order_by=Provenance.timestamp)
    provenance_history = relation(ProvenanceHistory, backref='diskfile', order_by=ProvenanceHistory.timestamp_start)

    # We use this to store an uncompressed Cache of a compressed file
    # This is not recorded in the database and is transient for the life
    # of this diskfile instance.
    uncompressed_cache_file = None

    # We store an astrodata instance here in the same way
    # These are expensive to instantiate
    # We instantiate  and close this externally though. It's stored here as it is
    # tightly linked to this actual diskfile, but obviously, this will not be set in
    # any DiskFile object returned by the ORM layer, or pulled in as a relation
    ad_object = None

    def __init__(self, given_file, given_filename, path, compressed=None):
        """
        Create a :class:`~Diskfile` record.

        Parameters
        ----------
        given_file : :class:`~File`
            A :class:`~File` record to associate with
        given_filename : str
            The name of the file
        path : str
            The path of the file within the `storage_root`
        compressed : bool
            True if the file is compressed.  It's also considered compressed if the filename ends in .bz2
        """
        self.file_id = given_file.id
        self.filename = given_filename
        self.path = path
        self.present = True
        self.canonical = True
        self.entrytime = datetime.datetime.now()
        self.file_size = self.get_file_size()
        self.file_md5 = self.get_file_md5()
        self.lastmod = self.get_lastmod()

        ts = _determine_timestamp_from_filename(given_filename)
        if ts is not None:
            self.datafile_timestamp = ts
        else:
            self.datafile_timestamp = self.lastmod

        if compressed == True or given_filename.endswith(".bz2"):
            self.compressed = True
            # Create the uncompressed cache filename and unzip to it
            try:
                if given_filename.endswith(".bz2"):
                    nonzfilename = given_filename[:-4]
                else:
                    nonzfilename = given_filename + "_bz2unzipped"
                self.uncompressed_cache_file = os.path.join(z_staging_area, nonzfilename)
                if os.path.exists(self.uncompressed_cache_file):
                    os.unlink(self.uncompressed_cache_file)

                os.system('bzcat %s > %s' % (self.fullpath(), self.uncompressed_cache_file))
                # TODO remove these lines once we are comfortable with the above
                # in_file = bz2.BZ2File(self.fullpath(), mode='rb')
                # out_file = open(self.uncompressed_cache_file, 'wb')
                # out_file.write(in_file.read())
                # in_file.close()
                # out_file.close()
            except:
                # Failed to create the unzipped cache file
                self.uncompressed_cache_file = None
                raise

            self.data_md5 = self.get_data_md5()
            self.data_size = self.get_data_size()
        else:
            self.compressed = False
            self.data_md5 = self.file_md5
            self.data_size = self.file_size

    def fullpath(self):
        """
        Get the full path to the file, including the `storage_root`, `path`, and `filename`

        Returns
        -------
            str : full path to file
        """
        return os.path.join(storage_root, self.path, self.filename)

    def get_file_size(self):
        """
        Get the size of the file

        Returns
        -------
            int : size of file in bytes
        """
        return os.path.getsize(self.fullpath())

    def exists(self):
        """
        Check if the file exists

        Returns
        -------
            bool : True if the file exits, is a file, and is readable, else False
        """
        exists = os.access(self.fullpath(), os.F_OK | os.R_OK)
        isfile = os.path.isfile(self.fullpath())
        return exists and isfile

    def get_file_md5(self):
        """
        Get the MD5 checksum of the file

        Returns
        -------
            str : md5 of the file as a string
        """
        return md5sum(self.fullpath())

    def get_data_md5(self):
        """
        Get the MD5 checksum of the uncompressed file.

        Returns:
            str : md5 of the uncompressed file (this may be the same if it is not compressed)
        """
        if self.compressed is False:
            return self.file_md5
        else:
            if self.uncompressed_cache_file:
                return md5sum(self.uncompressed_cache_file)
            else:
                (u_md5, u_size) = md5sum_size_bz2(self.fullpath())
                return u_md5

    def get_data_size(self):
        """
        Get the size of the uncompressed file

        Returns
        -------
            int : The size of the file when uncompressed.
        """
        if self.compressed is False:
            return self.get_file_size()
        else:
            if self.uncompressed_cache_file:
                return os.path.getsize(self.uncompressed_cache_file)
            else:
                (u_md5, u_size) = md5sum_size_bz2(self.fullpath())
                return u_size

    def get_lastmod(self):
        """
        Get the time the file was last modified

        Returns
        -------
            datetime : This checks on the filesystemf or the last modification date on the file
        """
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath()))

    def __repr__(self):
        """
        Get a string representation of this object

        Returns
        -------
            A human radable representation of this :class:`~Diskfile`
        """
        return "<DiskFile('%s', '%s', '%s', '%s')>" % (self.id, self.file_id, self.filename, self.path)