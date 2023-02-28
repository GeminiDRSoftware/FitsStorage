from sqlalchemy import Column, ForeignKey
from sqlalchemy import BigInteger, Integer, Text, Boolean, DateTime
from sqlalchemy.orm import relation, relationship

import os
import datetime
import bz2

from ..hashes import md5sum, md5sum_size_bz2
from ...logger import DummyLogger

from . import Base
from .file import File

from fits_storage.config import get_config
fsc = get_config()

__all__ = ["DiskFile"]

from .provenance import Provenance, ProvenanceHistory
# if fsc.is_server:
#     from ..server.preview import Preview

import astrodata
# DO NOT REMOVE THIS IMPORT, IT INITIALIZES THE ASTRODATA FACTORY
# noinspection PyUnresolvedReferences
import gemini_instruments      # pylint: disable=unused-import

try:
    import ghost_instruments
except:
    pass

class DiskFile(Base):
    """
    This is the ORM class for the diskfile table. A diskfile represents an
    instance of a file on disk. If the file is compressed (with bzip2) we
    keep some metadata on the actual file as is and also on the decompressed
    data. file_md5 and file_size are those of the actual file. data_md5 and
    data_size correspond to the uncompressed data if the file is compressed,
    and should be the same as for file_md5/file_size for uncompressed files.

    Parameters
    ----------
    given_file : :class:`~fits_storage_core.orm.file.File`
        A :class:`~fits_storage_core.orm.file.File` record to associate with
    given_filename : str
        The name of the file
    path : str
        The path of the file within the `storage_root`
    compressed : bool
        True if the file is compressed.  It's also considered compressed if
        the filename ends in .bz2
    """

    __tablename__ = 'diskfile'

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False, index=True)
    file = relation(File, order_by=id, back_populates='diskfiles')
    # if fsc.is_server
    #    previews = relationship(Preview, back_populates="diskfile",
    #    order_by=Preview.filename)

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

    #provenance = relationship(Provenance, back_populates='diskfile',
    #                          order_by=Provenance.timestamp)
    #provenance_history = relationship(ProvenanceHistory,
    #                                  back_populates='diskfile',
    #                                  order_by=ProvenanceHistory.timestamp_start)

    # We use this to store an uncompressed Cache of a compressed file
    # This is not recorded in the database and is transient for the life
    # of this diskfile instance.
    uncompressed_cache_file = None

    # We store an astrodata instance here in the same way These are expensive
    # to instantiate We instantiate  and close this externally though. It's
    # stored here as it is tightly linked to this actual diskfile,
    # but obviously, this will not be set in any DiskFile object returned by
    # the ORM layer, or pulled in as a relation
    ad_object = None

    # We store the items we use from the configuration system here
    # for convenience and to allow us to manipulate them for testing
    storage_root = fsc.storage_root
    z_staging_dir = fsc.z_staging_dir

    # Having the logger here is useful in ingest, but not valid e.g, when
    # in web code. DummyLogger() is a no-op default
    logger = DummyLogger()

    def __init__(self, given_file: File, given_filename: str, given_path: str,
                 compressed=None, logger=DummyLogger()):
        """
        Create a :class:`~fits_storage_core.orm.diskfile.DiskFile` record.

        Parameters
        ----------
        given_file : :class:`~fits_storage_core.orm.file.File`
            A :class:`~fits_storage_core.orm.file.File` record to associate with
        given_filename : str
            The name of the file
        path : str
            The path of the file within the `storage_root`
        compressed : bool
            True if the file is compressed.  It's also considered compressed
            if the filename ends in .bz2
        """
        self.file_id = given_file.id
        self.filename = given_filename
        self.path = given_path
        self.present = True
        self.canonical = True
        self.entrytime = datetime.datetime.now()
        self.file_size = self.get_file_size()
        self.file_md5 = self.get_file_md5()
        self.lastmod = self.get_file_lastmod()
        self.compressed = False

        if logger is not None:
            self.logger = logger

        self.ad_object = None

        if compressed is True or given_filename.endswith(".bz2"):
            self.compressed = True
            # Create the uncompressed cache filename and unzip to it
            try:
                if given_filename.endswith(".bz2"):
                    nonzfilename = given_filename[:-4]
                else:
                    nonzfilename = given_filename + "_bz2unzipped"
                self.uncompressed_cache_file = \
                    os.path.join(self.z_staging_dir, nonzfilename)
                if os.path.exists(self.uncompressed_cache_file):
                    os.unlink(self.uncompressed_cache_file)

                with bz2.open(self.fullpath, mode='rb') as ifp, \
                        open(self.uncompressed_cache_file, mode='wb') as ofp:
                    chunksize = 1000000  # 1E6
                    # TODO: use python 3.8 assignment expression
                    # while chunk := ifp.read(chunksize):
                    #    ofp.write(chunk)
                    chunk = ifp.read()
                    while chunk:
                        ofp.write(chunk)
                        chunk = ifp.read(chunksize)
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

    def cleanup(self):
        """
        Clean-up method for DiskFile class.
        Deletes the uncompressed cache file and ad_object if they exist
        """
        if self.ad_object is not None:
            try:
                self.ad_object.close()
                self.ad_object = None
            except:
                pass

        if self.uncompressed_cache_file is not None:
            try:
                os.unlink(self.uncompressed_cache_file)
            except:
                pass


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

    def get_data_md5(self):
        """
        Get the MD5 checksum of the uncompressed file.

        Returns
        -------
        str
            md5 of the uncompressed file (this may be the same if it is not
            compressed)
        """
        if self.compressed is False:
            return self.file_md5
        elif self.uncompressed_cache_file:
            return md5sum(self.uncompressed_cache_file)
        else:
            (u_md5, u_size) = md5sum_size_bz2(self.fullpath)
            return u_md5

    def get_data_size(self):
        """
        Get the size of the uncompressed file

        Returns
        -------
        int
            The size of the file when uncompressed.
        """
        if self.compressed is False:
            return self.get_file_size()
        elif self.uncompressed_cache_file:
            return os.path.getsize(self.uncompressed_cache_file)
        else:
            (u_md5, u_size) = md5sum_size_bz2(self.fullpath)
            return u_size

    def get_file_lastmod(self):
        """
        Get the time the file was last modified

        Returns
        -------
        datetime
            Reads the last modification date on the file from the filesystem
        """
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.fullpath))

    @property
    def get_ad_object(self):
        """
        Check if ad_object contains an astrodata object, return if so. If
        not, open the diskfile with AstroData and store the ad object in
        ad_object and return it.

        We don't call this from the constructor as at that point we're not sure
        that we have a fits file. We call this externally rather than opening
        the file directly with astrodata as that is expensive - this basically
        acts as a cache

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
