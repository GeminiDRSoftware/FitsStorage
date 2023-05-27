from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Enum

from .diskfile import DiskFile
from ...fits_verify import fitsverify

from fits_storage.logger import DummyLogger
from fits_storage.config import get_config

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

    def __init__(self, diskfile, skip_fv, skip_md, logger=DummyLogger(),
                 using_fitsverify=None,
                 fitsverify_path=None):
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
        self.logger = logger

        fsc = get_config()
        using_fitsverify = using_fitsverify if using_fitsverify is not None \
            else fsc.using_fitsverify
        fitsverify_path = fitsverify_path if fitsverify_path is not None \
            else fsc.fitsverify_path

        if skip_fv or not using_fitsverify:
            logger.debug("Skipping fits_verify")
            diskfile.fverrors = 0
        else:
            logger.debug("Calling fits_verify")
            self.fits_verify(diskfile, fvpath=fitsverify_path)

        if skip_md:
            logger.debug("Skipping Metadata validation")
            diskfile.mdready = True
        else:
            logger.debug("Calling Metadata validator")
            self.md(diskfile)

    def fits_verify(self, diskfile, fvpath=None):
        """
        Calls the fits_verify module and records the results.

        - Populates the isfits, fverrors and fvwarnings in the diskfile object
          passed in.

        - Populates the fvreport in self

        We pass fvpath on to the fitsverify module. If it contains anything,
        it will be treated as the path to the fitsverify executable. If not,
        the fitsverify module will search $PATH for it.

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            Run FITS verify report on this :class:`~DiskFile`

        fvpath : str or None
            Path to the fitsverify executable.
        """
        filename = diskfile.get_uncompressed_file()

        try:
            retlist = fitsverify(filename, fvpath=fvpath)
        except Exception:
            self.logger.error("Exception during fitsverify", exc_info=True)

        diskfile.isfits = bool(retlist[0])
        diskfile.fvwarnings = retlist[1]
        diskfile.fverrors = retlist[2]
        self.fvreport = retlist[3]

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
        filename = diskfile.get_uncompressed_file()

        try:
            result = evaluate(filename)
            diskfile.mdready = result.passes
            self.mdstatus = result.code
            if result.message is not None:
                self.mdreport = result.message
        except Exception as e:
            # don't want to fail the ingest over a metadata report
            self.logger.error("Exception during metadata validation",
                              exc_info=True)
