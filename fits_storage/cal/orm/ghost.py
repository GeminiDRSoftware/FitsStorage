from sqlalchemy import Column, ForeignKey, Numeric
from sqlalchemy import Integer, Text, Boolean, Enum
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base
from fits_storage.core.orm.header import Header

# Enumerated column types
READ_SPEED_SETTINGS = ['slow', 'medium', 'fast', 'standard']
READ_SPEED_SETTING_ENUM = Enum(*READ_SPEED_SETTINGS,
                               name='ghost_read_speed_setting')

GAIN_SETTINGS = ['low', 'high', 'standard']
GAIN_SETTING_ENUM = Enum(*GAIN_SETTINGS, name='ghost_gain_setting')

ARMS = ['blue', 'red', 'slitv']
ARMS_ENUM = Enum(*ARMS, name='ghost_arm')

FPMS = ['SR', 'HR']
FPMS_ENUM = Enum(*FPMS, name='ghost_focal_plane_masks')

GHOST_ARM_DESCRIPTORS = [
    "detector_name",
    "detector_x_bin",
    "detector_y_bin",
    "exposure_time",
    "gain_setting",
    "read_speed_setting",
    "focal_plane_mask",
]

GHOST_ARMS = ["blue", "red", "slitv"]

class Ghost(Base):
    """
    This is the ORM object for the GHOST details. This one is a little different
    to the other instruments in that we add one row to this table for every
    *arm* in every ghost file, so there is typically a 1:3 mapping of
    header:ghost for raw data, and a 1:1 mapping for reduced data where the
    ghost data are "unbundled".

    This works fine because when searching for calibrations, it will find any
    rows in the ghost table that match. Some of those may point to the same
    header_id and the header_id is really the result you're searching for.
    """
    __tablename__ = 'ghost'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                       index=True)
    header = relationship(Header, order_by=id)
    arm = Column(ARMS_ENUM, index=True, nullable=False)
    want_before_arc = Column(Boolean)

    detector_name = Column(Text, index=True)
    detector_x_bin = Column(Integer, index=True)
    detector_y_bin = Column(Integer, index=True)
    exposure_time = Column(Numeric(precision=8, scale=4))
    gain_setting = Column(GAIN_SETTING_ENUM, index=True)
    read_speed_setting = Column(READ_SPEED_SETTING_ENUM, index=True)
    focal_plane_mask = Column(FPMS_ENUM, index=True)

    prepared = Column(Boolean, index=True)
    overscan_subtracted = Column(Boolean, index=True)
    overscan_trimmed = Column(Boolean, index=True)

    def __init__(self, header, ad, arm):
        """
        Create a Ghost instrument record corresponding to the given arm in the
        given ad. Note that the ad may or may not have multiple arms, and if it
        is a single arm, it may not be the arm that was asked for, in which
        case we set self.arm to None which will prevent adding this instance to
        the database.

        Parameters
        ----------
        header : :class:`~header.Header`
            Corresponding header for the observation
        ad : :class:`astrodata.AstroData`
            Astrodata object to load Ghost information from
        arm: name of the ghost arm you want a record for.
        """
        self.header = header

        # Populate from the astrodata object
        self.populate(ad, arm)

    def populate(self, ad, arm):
        """
        Populate the Ghost information from the given
        :class:`astrodata.AstroData`

        Parameters
        ----------
        ad : :class:`astrodata.Astrodata`
            Astrodata object to read Ghost information from
        """
        if arm not in GHOST_ARMS:
            raise ValueError(f"Invalid GHOST arm: {arm}")

        if ad.arm() is None:
            # We have a multi-arm "bundle" ad
            self.arm = arm
            self.populate_from_multi(ad)
        else:
            # We have a single arm ad
            if ad.arm() != arm:
                # But it's for an arm we don't want
                self.arm = None
                return
            else:
                # Single arm ad for the arm we want.
                self.arm = arm
                self.populate_from_single(ad)

        self.want_before_arc = ad.want_before_arc()

        self.prepared = 'PREPARED' in ad.tags
        self.overscan_trimmed = 'OVERSCAN_TRIMMED' in ad.tags
        self.overscan_subtracted = 'OVERSCAN_SUBTRACTED' in ad.tags

    def populate_from_single(self, ad):
        for desc in GHOST_ARM_DESCRIPTORS:
            setattr(self, desc, getattr(ad, desc)())

    def populate_from_multi(self, ad):
        for desc in GHOST_ARM_DESCRIPTORS:
            value = getattr(ad, desc)()
            if isinstance(value, dict):
                value = value[self.arm]
            setattr(self, desc, value)
