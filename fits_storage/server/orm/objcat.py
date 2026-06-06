import numpy
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Float

from fits_storage.core.orm import Base

from fits_storage.logger_dummy import DummyLogger


class Objcat(Base):
    """
    This is the ORM object for the objcat table, that captures entries from
    .OBJCAT tables in reduced imaging data.
    """

    __tablename__ = 'objcat'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                         index=True)
    extnum = Column(Integer, nullable=False)

    # These are the sextractor column names, which go directly into the objcat
    NUMBER = Column(Integer)  # Running ID number
    X_IMAGE = Column(Float) # Detection barycenter in pixel coordinates
    Y_IMAGE = Column(Float) # Detection barycenter in pixel coordinates
    ERRX2_IMAGE = Column(Float) # Variance estimate on X_IMAGE
    ERRY2_IMAGE = Column(Float) # Variance estimate on Y_IMAGE
    ERRXY_IMAGE = Column(Float) # Co-Variance estimate X_IMAGE,Y_IMAGE
    X_WORLD = Column(Float) # Detection barycenter in world coordinates
    Y_WORLD = Column(Float) # Detection barycenter in world coordinates
    ERRX2_WORLD = Column(Float) # Variance estimate on X_WORLD
    ERRY2_WORLD = Column(Float) # Variance estimate on Y_WORLD
    ERRXY_WORLD = Column(Float) # Co-Variance estimate on X_WORLD,Y_WORLD

    A_IMAGE = Column(Float) # Semi-major axis length in pixels
    B_IMAGE = Column(Float) # Semi-minor axis length in pixels
    THETA_IMAGE = Column(Float) # Position anlge between semi-major axis and NAXIS1 image axis, measures counter-clockwise
    ERRA_IMAGE = Column(Float) # Variance on A_IMAGE
    ERRB_IMAGE = Column(Float) # Variance on B_IMAGE
    ERRTHETA_IMAGE = Column(Float) # Variance on THETA_IMAGE
    A_WORLD = Column(Float) # Semi-major axis length in degrees
    B_WORLD = Column(Float) # Semi-minor axis length in degrees
    THETA_WORLD = Column(Float) # Position anlge between semi-major axis and 1st axis of WCS
    ERRA_WORLD = Column(Float) # Variance on A_WORLD
    ERRB_WORLD = Column(Float) # Variance on B_WORLD
    ERRTHETA_WORLD = Column(Float) # Variance on THETA_WORLD
    FWHM_IMAGE = Column(Float) # FWHM in pixels
    FWHM_WORLD = Column(Float) # FWHM in degrees
    FLUX_RADIUS = Column(Float) # EE50 radius
    ELLIPTICITY = Column(Float) # Ellipticity

    FLUX_AUTO = Column(Float) # Flux measurement, automatically sized apperture
    FLUXERR_AUTO = Column(Float) # Standard Deviation on FLUX_AUTO
    MAG_AUTO = Column(Float) # As FLUX_AUTO but in magnitudes
    MAGERR_AUTO = Column(Float) # Standard Deviation on MAG_AUTO
    FLUX_MAX = Column(Float) # Peak flux level. We'll need this to calculate Strehl ratio

    CLASS_STAR = Column(Float)

    ISOAREA_IMAGE = Column(Integer) # Number of pixels covered by object.
    FLAGS = Column(Integer) # Sextractor detection flags
    IMAFLAGS_ISO = Column(Integer) # DQ values from data over area of object
    NIMAFLAGS_ISO = Column(Integer) # number of pixles with DQ values from data over area of object

    BACKGROUND = Column(Float)

    # These are populated in files where there is a corresponding REFCAT

    REF_NUMBER = Column(Integer)
    REF_MAG = Column(Float)
    REF_MAG_ERR = Column(Float)
    PROFILE_FWHM = Column(Float)
    PROFILE_EE50 = Column(Float)

    def __init__(self, header_id, extnum, row, logger=DummyLogger()):
        # Populate objcat columns from a header_id and objact table row
        self.header_id = header_id
        self.extnum = extnum

        # Somewhat brute force approach.
        for key in row.keys():
            if hasattr(self, key):
                # Need to do some type munging...
                value = row[key]
                if isinstance(value, numpy.integer):
                    value = int(value)
                elif isinstance(value, numpy.floating):
                    value = float(value)
                else:
                    logger.debug(f"Did not adapt type for objcat {key}")
                self.__setattr__(key, value)
            else:
                logger.warning(f"OBJCAT table has column {key} that is not "
                               f"present in objcat ORM instance")
