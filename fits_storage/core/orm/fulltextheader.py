from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from fits_storage.core.orm import Base
from fits_storage.core.orm.diskfile import DiskFile


class FullTextHeader(Base):
    """
    This is the ORM object for the Full Text of the header.
    We keep this is a separate table from Header to improve DB performance.

    """
    __tablename__ = 'fulltextheader'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey(DiskFile.id), nullable=False,
                         index=True)
    fulltext = Column(Text)

    def __init__(self, diskfile):
        """
        Create a :class:`~FullTextHeader` record for the given file

        Parameters
        ----------
        diskfile : :class:`~diskfile.DiskFile`
            File on disk to read header from
        """
        self.diskfile_id = diskfile.id
        self.populate(diskfile)

    def populate(self, diskfile):
        """
        Populate the FullTextHeader data items.

        Parameters
        ----------
        diskfile : :class:`~diskfile.DiskFile`
            Read the header out of this diskfile to populate the record
        """
        ad = diskfile.get_ad_object

        self.fulltext = ""
        self.fulltext += "Filename: " + diskfile.filename + "\n\n"
        self.fulltext += "AstroData Tags: " + str(ad.tags) + "\n\n"
        self.fulltext += "\n--- PHU ---\n"    
        self.fulltext += repr(ad.phu).strip()
        self.fulltext += "\n"
        for i in range(len(ad)):
            # We label these as "i+1" so that the PHU is 0 and the extensions
            # start from 1.
            self.fulltext += f"\n--- HDU {i+1} ---\n"
            self.fulltext += repr(ad[i].hdr).strip()
            self.fulltext += '\n'
