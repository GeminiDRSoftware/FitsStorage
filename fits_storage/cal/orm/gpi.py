from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Boolean
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base
from fits_storage.core.orm.header import Header


__all__ = ["Gpi"]


# ------------------------------------------------------------------------------
class Gpi(Base):
    """
    This is the ORM object for the GPI details.

    Parameters
    ----------
    header : :class:`~fits_storage_core.orm.header.Header`
        Corresponding header record for the GPI data
    ad : :class:`~astrodata.core.AstroData`
        AstroData object to read GPI information from
    """
    __tablename__ = 'gpi'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                       index=True)
    header = relationship(Header, order_by=id)
    filter_name = Column(Text, index=True)
    disperser = Column(Text, index=True)
    focal_plane_mask = Column(Text, index=True)
    pupil_mask = Column(Text, index=True)
    astrometric_standard = Column(Boolean, index=True)
    wollaston = Column(Boolean, index=True)
    prism = Column(Boolean, index=True)

    def __init__(self, header: Header, ad):
        """
        Record for GPI information

        Parameters
        ----------
        header : :class:`~fits_storage_core.orm.header.Header`
            Corresponding header record for the GPI data
        ad : :class:`~astrodata.core.AstroData`
            AstroData object to read GPI information from
        """
        self.header = header

        # Populate from an astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate this GPI record from the given AstroData object

        Parameters
        ----------
        ad : :class:`astrodata.core.AstroData`
            AstroData object to populate GPI data from
        """
        self.filter_name = ad.filter_name()
        self.disperser = ad.disperser()
        self.focal_plane_mask = ad.focal_plane_mask()
        self.pupil_mask = ad.pupil_mask()
        self.astrometric_standard = ad.phu.get("ASTROMTC")
        if self.disperser is not None:
            self.wollaston = 'WOLLASTON' in self.disperser
            self.prism = 'PRISM' in self.disperser 
