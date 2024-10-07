import sys

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from . import Base
from .header import Header

from fits_storage.logger import DummyLogger

class Footprint(Base):
    """
    This is the ORM object for the Footprint table. Each row is a footprint
    derived from a WCS. There can be several footprints (typically one per
    science extension) per header object.

    """
    __tablename__ = 'footprint'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey(Header.id), nullable=False,
                       index=True)
    extension = Column(Text)
    # An area column of type polygon gets added using raw sql in
    # CreateTables.py

    def __init__(self, header, logger=DummyLogger()):
        """
        Create a :class:`~Footprint`

        Parameters
        ----------
        header : :class:`~header.Header`
            Corresponding header record for this footprint
        """
        self.header_id = header.id
        self.logger = logger


# Note, this function is not a member of the ORM class. This seemed the best
# place for it to live though.
def footprints(ad, logger=DummyLogger()):
    """
    Generate a list of footprints from an astrodata object. We get one
    footprint per extension.

    Parameters
    ----------
    ad - astrodata instance

    Returns
    -------
    A dict of footprints. Each key is an identifier of the extension, and the
    value is a list of tuples which themselves are the (RA, Dec) coordinates
    of the corners of the footprint.
    """

    footprints = {}
    for ext in ad:
        # The detector section
        sec = ext.data_section()
        if sec is None:
            continue

        extname = ext.hdr.get('EXTNAME', 'Unknown')
        extver = ext.hdr.get('EXTVER', 'Unknown')
        label = f"{extname}-{extver}"
        # Ensure the label is unique
        while label in footprints.keys():
            label += 'x'
        try:
            # If we're not in an RA/Dec frame, don't even bother
            if (ext.hdr.get('CTYPE1') == 'RA---TAN') and \
                    (ext.hdr.get('CTYPE2') == 'DEC--TAN'):
                # Note that the format of the "bounding box" argument to
                # gwcs.wcs.footprint is ((x1, x2), (y1, y2)).
                pix_bb = ((sec.x1, sec.x2), (sec.y1, sec.y2))
                wc_fp = ext.wcs.footprint(pix_bb)
                footprints[label] = wc_fp
        except:
            # TODO: silently handle lame duck exceptions here
            logger.debug("Footprint Exception", exc_info=True)
    return footprints

