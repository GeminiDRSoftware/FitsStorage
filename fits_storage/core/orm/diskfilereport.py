from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum

import os

from .diskfile import DiskFile
from ...fits_verify import fitsverify

from . import Base

from ...fits_validator.gemini_fits_validator import AstroDataEvaluator, STATUSES

evaluate = AstroDataEvaluator()

STATUS_ENUM = Enum(*STATUSES, name='mdstatus')


class DiskFileReport(Base):
    """
    This is the ORM object for DiskFileReport.
    Contains the Fits Verify and WMD reports for a diskfile
    These can be fairly large chunks of text, so we split this
    out from the DiskFile table for DB performance reasons

    When we instantiate this class, we pass it the diskfile object.
    This class will update that diskfile object with the fverrors and mdready
    values, but will not commit the changes.
    """
    __tablename__ = 'diskfilereport'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey(DiskFile.id), nullable=False,
                         index=True)
    fvreport = Column(Text)
    mdreport = Column(Text)
    mdstatus = Column(STATUS_ENUM, index=True)

    def __init__(self, diskfile, skip_fv, skip_md):
        """
        Create a :class:`~DiskFileReport` for the given :class:`~DiskFile`
        by running the FITS and metadata checks.

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            Run the reports on this :class:`~DiskFile`
        skip_fv : bool
            If `True`, skip the FITS verication report
        skip_md : bool
            If `True`, skip the Metadata verification report
        """
        self.diskfile_id = diskfile.id
        if skip_fv:
            diskfile.fverrors = 0
        else:
            try:
                self.fits_verify(diskfile)
            except:
                # stub out for testing locally w/o fv
                diskfile.fverrors = 0
        if skip_md:
            diskfile.mdready = True
        else:
            self.md(diskfile)

    def fits_verify(self, diskfile):
        """
        Calls the fits_verify module and records the results.

        - Populates the isfits, fverrors and fvwarnings in the diskfile object
          passed in.

        - Populates the fvreport in self

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            Run FITS verify report on this :class:`~DiskFile`
        """
        filename = None
        if diskfile.compressed:
            if diskfile.uncompressed_cache_file and \
                    os.access(diskfile.uncompressed_cache_file,
                              os.F_OK | os.R_OK):
                filename = diskfile.uncompressed_cache_file
            else:
                # For now, we do not support fitsverify of compressed files
                # if we are not using the diskfile.uncompressed_cache_file
                filename = None
        else:
            # not compressed - just use the diskfile filename
            filename = diskfile.fullpath

        if filename:
            retlist = fitsverify(filename)
            diskfile.isfits = bool(retlist[0])
            diskfile.fvwarnings = retlist[1]
            diskfile.fverrors = retlist[2]
            # If the FITS file has bad strings in it, fitsverify will quote
            # them in the report, and the database will object to the bad
            # characters in the unicode string - errors=ignore makes it
            # ignore these. O - looks like a bad conversion for python3
            # support, let's try keeping this simple
            if isinstance(retlist[3], str):
                self.fvreport = retlist[3]
            else:
                self.fvreport = retlist[3].decode("utf-8", errors='replace')

    def md(self, diskfile):
        """
        Evaluates the headers and records the md results

        - Populates the mdready flag in the diskfile object passed in
        - Populates the mdreport text in self
        - Populates the mdstatus enum in self

        Prameters
        ---------
        diskfile : :class:`~DiskFile`
            Run the Metadata report on this :class:`~DiskFile`
        """
        filename = None
        if diskfile.compressed:
            if diskfile.uncompressed_cache_file and \
                    os.access(diskfile.uncompressed_cache_file,
                              os.F_OK | os.R_OK):
                filename = diskfile.uncompressed_cache_file
            else:
                # For now, we do not support fitsverify of compressed files
                # if we are not using the diskfile.uncompressed_cache_file
                filename = None
        else:
            # not compressed - just use the diskfile filename
            filename = diskfile.fullpath()

        if filename:
            try:
                result = evaluate(diskfile.ad_object)
                diskfile.mdready = result.passes
                self.mdstatus = result.code
                if result.message is not None:
                    self.mdreport = result.message
            except Exception as e:
                # don't want to fail the ingest over a metadata report
                pass