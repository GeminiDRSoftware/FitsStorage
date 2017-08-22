from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

import astrodata
import gemini_instruments

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
        """
        Populate the FullTextHeader data items
        """
        # The fulltextheader object is unusual in that we directly pass the constructor a diskfile
        # object which may have an ad_object in it.
        if diskfile.ad_object is not None:
            ad = diskfile.ad_object
        else:
            if diskfile.uncompressed_cache_file:
                fullpath = diskfile.uncompressed_cache_file
            else:
                fullpath = diskfile.fullpath()
            ad = astrodata.open(fullpath)

        self.fulltext = ""
        self.fulltext += "Filename: " +  diskfile.filename + "\n\n"
        self.fulltext += "AstroData Types: " +str(ad.tags) + "\n\n"
        for i in range(len(ad.hdulist)):
            self.fulltext += "\n--- HDU %s ---\n" % i
            self.fulltext += unicode(repr(ad.hdulist[i].header), errors='replace')
            self.fulltext += '\n'
