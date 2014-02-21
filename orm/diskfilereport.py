from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from fits_verify import fitsverify

from fits_storage_config import using_cadc

if(using_cadc):
    import Cadc

from . import Base

class DiskFileReport(Base):
    """
    This is the ORM object for DiskFileReport.
    Contains the Fits Verify and WMD reports for a diskfile
    These can be fairly large chunks of text, so we split this
    out from the DiskFile table for DB performance reasons

    When we instantiate this class, we pass it the diskfile object.
    This class will update that diskfile object with the fverrors and wmdready
    values, but will not commit the changes.
    """
    __tablename__ = 'diskfilereport'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    fvreport = Column(Text)
    wmdreport = Column(Text)


    def __init__(self, diskfile, skip_fv, skip_wmd):
        self.diskfile_id = diskfile.id
        if(skip_fv or not using_cadc):
            diskfile.fverrors = 0
        else:
            self.fits_verify(diskfile)
        if(skip_wmd or not using_cadc):
            diskfile.wmdready = True
        else:
            self.wmd(diskfile)

    def fits_verify(self, diskfile):
        """
        Calls the fits_verify module and records the results.
        - Populates the isfits, fverrors and fvwarnings in the diskfile object
          passed in
        - Populates the fvreport in self
        """
        retlist = fitsverify(diskfile.fullpath())
        diskfile.isfits = bool(retlist[0])
        diskfile.fvwarnings = retlist[1]
        diskfile.fverrors = retlist[2]
        # If the FITS file has bad strings in it, fitsverify will quote them in 
        # the report, and the database will object to the bad characters in 
        # the unicode string - errors=ignore makes it ignore these.
        self.fvreport = unicode(retlist[3], errors='replace')

    def wmd(self, diskfile):
        """
        Calls the Cadc module and records the wmd results
        - Populates the wmdready flag in the diskfile object passed in
        - Populates the wmdreport text in self
        """
        retlist = Cadc.cadcWMD(diskfile.fullpath())
        diskfile.wmdready = bool(retlist[0])
        self.wmdreport = retlist[1]
