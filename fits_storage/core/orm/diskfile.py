from sqlalchemy import Column, ForeignKey
from sqlalchemy import BigInteger, Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relationship

import os
import datetime
import bz2
import tempfile
import hashlib

from fits_storage.core.hashes import md5sum
from fits_storage.logger import DummyLogger

from . import Base

from fits_storage.config import get_config

__all__ = ["DiskFile"]

fsc = get_config()
if fsc.is_server:
    from fits_storage.server.orm.provenancehistory import Provenance, History

import astrodata
# DO NOT REMOVE THIS IMPORT, IT INITIALIZES THE ASTRODATA FACTORY
# noinspection PyUnresolvedReferences
import gemini_instruments      # pylint: disable=unused-import


class DiskFile(Base):
    """
    This is the ORM class for the diskfile table. A diskfile represents an
    instance of a file on disk. If the file is compressed (with bzip2) we
    keep some metadata on the actual file as is and also on the decompressed
    data. file_md5 and file_size are those of the actual file. data_md5 and
    data_size correspond to the uncompressed data if the file is compressed,
    and should be the same as for file_md5/file_size for uncompressed files.

    This class also provides a number of utility methods for interacting with
    the actual file itself, for example getting the full path to the file,
    measuring the size, calculating the md5sum etc. These are used both
    internally (within this class) and externally. In general, methods with
    names such as file_md5 are ORM methods that reflect database columns.
    Methods that start with get_ (e.g. get_file_md5) actually calculate the
    value from the actual file on disk.

    Finally, this class provides facilities for fetching the file from AWS S3
    if needed, for storing a "cached" uncompressed version of compressed
    data, and even for storing an open astrodata instance of the file.
     here are several operations that need access to the uncompressed
    file and uncompressing it each time is a significant performance hit, as
    is opening the file with astrodata.
    Functionality such as fetching from S3, creating an uncompressed cache
    file, or opening the file with astrodata are all lazy - they are done on
    demand, but the result is stored for future use. There is a cleanup()
    method which will clean up all temporary files. This has to be called
    manually. We can't use __del__ nicely with SQLAlchemy orm objects.
    """

    __tablename__ = 'diskfile'

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
    file = relationship("File", order_by=id, back_populates='diskfiles')

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

    if fsc.is_server:
        provenance = relationship(Provenance, back_populates='diskfile',
                                  order_by=Provenance.timestamp)
        history = relationship(History, back_populates='diskfile',
                               order_by=History.timestamp_start)
    #   previews = relationship(Preview, back_populates="diskfile",
    #                           order_by=Preview.filename)

    # We use this to store an uncompressed Cache of a compressed file
    # This is not recorded in the database and is transient for the life
    # of this diskfile instance.
    uncompressed_cache_file = None

    # We use this to store the location of a file we fetched from S3. This will
    # usually be compressed, so most clients will use uncompressed_cache_file
    # in preference to this.
    local_copy_of_s3_file = None

    # We store an astrodata instance here in the same way These are expensive
    # to instantiate We instantiate  and close this externally though. It's
    # stored here as it is tightly linked to this actual diskfile,
    # but obviously, this will not be set in any DiskFile object returned by
    # the ORM layer, or pulled in as a relation
    ad_object = None

    # We store some fsc values to allow poking for testing etc.
    # Declare them here, set in __init__ or as needed
    _storage_root = None
    _z_staging_dir = None
    _s3_staging_dir = None

    def __init__(self, given_file, given_filename: str, given_path: str,
                 compressed=None, logger=DummyLogger(),
                 storage_root=None, z_staging_dir=None, s3_staging_dir=None):
        """
        Create a :class:`~fits_storage_core.orm.diskfile.DiskFile` record.

        Parameters
        ----------
        given_file : :class:`~fits_storage_core.orm.file.File`
            A :class:`~fits_storage_core.orm.file.File` record to associate with
        given_filename : str
            The name of the file
        given_path : str
            The path of the file within the `storage_root`
        compressed : bool
            True if the file is compressed.  It's also considered compressed
            if the filename ends in .bz2
        logger: python logger instance or FitsStorage DummyLogger instance
        storage_root : str or None
        z_staging_dir : str or None
        s3_staging_dir : str or None
            These three are provided here to they can be overridden for
            testing and debugging. They default to None which causes them
            to take the values from the configuration system.
        """
        fsc = get_config()
        # We store the items we use from the configuration system in the class
        # for convenience and to allow us to manipulate them for testing.
        self._storage_root = storage_root if storage_root is not None \
            else fsc.storage_root
        self._z_staging_dir = z_staging_dir if z_staging_dir is not None \
            else fsc.z_staging_dir
        self._s3_staging_dir = s3_staging_dir if s3_staging_dir is not None \
            else fsc.s3_staging_dir

        # Having the logger here is useful in ingest, but not valid e.g, when
        # in web code. DummyLogger() is a no-op default
        self._logger = logger

        self.file_id = given_file.id
        self.filename = given_filename
        self.path = given_path
        self.present = True
        self.canonical = True
        self.entrytime = datetime.datetime.now()
        self.file_size = os.path.getsize(self.fullpath)
        self.file_md5 = self.get_file_md5()
        self.lastmod = self.get_file_lastmod()
        self.compressed = False

        self.uncompressed_cache_file = None
        self.ad_object = None

        if compressed is True or given_filename.endswith(".bz2"):
            self.compressed = True
            self.uncompressed_cache_file = self.get_uncompressed_file()
            # This also populates data_size and data_md5.
        else:
            self.compressed = False
            self.data_md5 = self.file_md5
            self.data_size = self.file_size

    # These need to be properties so that they self-initialize - instances
    # of this class returned by the sqlalchemy layer do not get __init__ed.
    @property
    def storage_root(self):
        if self._storage_root is None:
            fsc = get_config()
            self._storage_root = fsc.storage_root
        return self._storage_root

    @property
    def z_staging_dir(self):
        if self._z_staging_dir is None:
            fsc = get_config()
            self._z_staging_dir = fsc.z_staging_dir
        return self._z_staging_dir

    @property
    def s3_staging_dir(self):
        if self._s3_staging_dir is None:
            fsc = get_config()
            self._s3_staging_dir = fsc.s3_staging_dir
        return self._s3_staging_dir

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = DummyLogger()
        return self._logger

    def get_uncompressed_file(self, compute_values=True):
        if self.uncompressed_cache_file is not None:
            return self.uncompressed_cache_file

        if self.compressed is False:
            self.uncompressed_cache_file = self.fullpath
            return self.fullpath

        try:
            # Create a filename for the uncompressed version
            tmpfile = tempfile.NamedTemporaryFile(dir=self.z_staging_dir,
                                                  delete=False)

            # Full path to it in the z_staging dir
            self.uncompressed_cache_file = tmpfile.name

            self.logger.debug("Creating uncompressed_cache_file "
                              f"{self.uncompressed_cache_file} for "
                              f"{self.filename}")

            # By default, we calculate the data_size and data_md5
            data_size = 0
            hashobj = hashlib.md5()
            with bz2.open(self.fullpath, mode='rb') as ifp:
                chunksize = 1000000  # 1 MByte
                # TODO: use python 3.8 assignment expression
                while True:
                    chunk = ifp.read(chunksize)
                    if not chunk:
                        break
                    tmpfile.write(chunk)
                    if compute_values:
                        data_size += len(chunk)
                        hashobj.update(chunk)
            tmpfile.close()  # Note it's created with delete=False
            if compute_values:
                self.data_size = data_size
                self.data_md5 = hashobj.hexdigest()

        except:
            # Failed to create the unzipped cache file
            self.uncompressed_cache_file = None
            self.logger.error("Exception creating uncompressed_cache_file "
                              f"{self.uncompressed_cache_file} from "
                              f"{self.fullpath} for "
                              f"{self.filename}", exc_info=True)
            raise

        return self.uncompressed_cache_file

    def cleanup(self):
        """
        Clean-up method for DiskFile class.
        Deletes the uncompressed cache file and ad_object if they exist
        """
        if self.ad_object is not None:
            self.logger.debug("Closing diskfile.ad_object. Well, there's "
                              "actually no ad.close() method, so we just "
                              "set ad_object=None and hope for the best")
            # self.ad_object.close()
            self.ad_object = None

        if self.uncompressed_cache_file is not None and \
                self.uncompressed_cache_file != \
                os.path.join(self._storage_root, self.path, self.filename):
            self.logger.debug("Deleting uncompressed_cache_file "
                              f"{self.uncompressed_cache_file}")
            try:
                os.unlink(self.uncompressed_cache_file)
            except (PermissionError, FileNotFoundError):
                pass

            self.uncompressed_cache_file = None

    @property
    def fullpath(self):
        """
        Get the full path to the file, including the `storage_root`, `path`,
        and `filename`

        Returns
        -------
        str
            full path to file
        """
        return os.path.join(self.storage_root, self.path, self.filename)

    def get_file_size(self):
        """
        Get the size of the file

        Returns
        -------
        int
            size of file in bytes
        """
        return os.path.getsize(self.fullpath)

    def get_file_lastmod(self):
        """
        Get the lastmod datetime of the file

        Returns
        -------
        datetime.datetime
            - the lastmod (mtime) of the file on disk
        """
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath))

    def file_exists(self):
        """
        Check if the file exists

        Returns
        -------
        bool
            True if the file exits, is a file, and is readable, else False
        """
        exists = os.access(self.fullpath, os.F_OK | os.R_OK)
        isfile = os.path.isfile(self.fullpath)
        return exists and isfile

    def get_file_md5(self):
        """
        Get the MD5 checksum of the file

        Returns
        -------
        str
            md5 of the file as a string
        """
        return md5sum(self.fullpath)

    @property
    def get_ad_object(self):
        """
        Check if ad_object contains an astrodata object, return it if so. If
        not, open the uncompressed_cache_file with AstroData and store the
        ad object in ad_object and return it.

        We don't call this from the constructor as at that point we're not sure
        that we have a fits file or that we need an ad_object. We call this
        externally rather than opening the file directly with astrodata as
        that is expensive - this basically acts as a cache

        Returns
        -------
        astrodata object
        """
        if self.ad_object is not None:
            self.logger.debug('Using ad_object from diskfile')
            return self.ad_object

        if self.uncompressed_cache_file:
            self.logger.debug("Using uncompressed cache file from diskfile")
            fullpath = self.uncompressed_cache_file
        else:
            fullpath = self.fullpath
        try:
            self.logger.debug(f"Opening {fullpath} with AstroData and storing "
                              "to diskfile ad_object")
            self.ad_object = astrodata.open(fullpath)
            return self.ad_object
        except:
            self.logger.error(f"Error opening {fullpath} with AstroData")
            return None

    def __repr__(self):
        """
        Get a string representation of this object

        Returns
        -------
        str
            A human readable representation of this
            :class:`~fits_storage_core.orm.diskfile.DiskFile`
        """
        return f"<DiskFile({self.id}, {self.file_id}, {self.filename}, " \
               f"{self.path})>"
