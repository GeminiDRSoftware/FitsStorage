from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from astrodata import AstroData

from . import Base

class FullTextHeader(Base):
    """
    This is the ORM object for the Full Text of the header.
    We keep this is a separate table from Header to improve DB performance
    """
    __tablename__ = 'fulltextheader'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    fulltext = Column(Text)

    def __init__(self, diskfile):
        self.diskfile_id = diskfile.id
        self.populate(diskfile)

    def populate(self, diskfile):
        fullpath = diskfile.fullpath()
        # Try and open it as a fits file
        ad = 0
        try:
            ad = AstroData(fullpath, mode='readonly')
            self.fulltext = ""
            self.fulltext += "Filename: " +  diskfile.filename + "\n\n"
            self.fulltext += "AstroData Types: " +str(ad.types) + "\n\n"
            for i in range(len(ad.hdulist)):
                self.fulltext += "\n--- HDU %s ---\n" % i
                self.fulltext += unicode(str(ad.hdulist[i].header.ascardlist()), errors='replace')
                self.fulltext += '\n'
            ad.close()

        except:
            # Astrodata open failed or there was some other exception
            ad.close()
            raise
