from abc import abstractmethod

from sqlalchemy import Column, ForeignKey, Numeric
from sqlalchemy import Integer, Text, Boolean, Enum
from sqlalchemy.orm import relation

from .header import Header

from . import Base

# Enumerated column types
READ_SPEED_SETTINGS = ['slow', 'medium', 'fast', 'standard', 'unknown']
READ_SPEED_SETTING_ENUM = Enum(*READ_SPEED_SETTINGS, name='ghost_read_speed_setting')

GAIN_SETTINGS = ['low', 'high', 'standard']
GAIN_SETTING_ENUM = Enum(*GAIN_SETTINGS, name='ghost_gain_setting')

RESOLUTIONS = ['std', 'high']
RESOLUTION_ENUM = Enum(*RESOLUTIONS, name='ghost_resolution')


def _exptfix(val) -> int:
    if isinstance(val, str):
        try:
            val = int(str)
        except:
            val = None
    if val is not None and (10000 < val or val < 0):
        val = None
    return val


def _gain_check(val):
    return val if val in GAIN_SETTINGS else None


def _read_speed_check(val):
    return val if val in READ_SPEED_SETTINGS else None


def _parse_amp_read_area(ara):
    if ara is None:
        return None
    else:
        return '+'.join(ara)


def _exptime_bundle(val):
    expt = 0
    if isinstance(val, dict):
        for arm, arm_expt in val.items():
            expt += _exptfix(arm_expt)
    else:
        expt = val
    return expt


_conversion_functions = {
    "exposure_time": _exptfix,
    "gain_setting": _gain_check,
    "read_speed_setting": _read_speed_check,
    "amp_read_area": _parse_amp_read_area
}


_bundle_functions = {
    "exposure_time": _exptime_bundle,
}


GHOST_ARM_DESCRIPTORS = [
    "detector_name",
    "detector_x_bin",
    "detector_y_bin",
    "exposure_time",
    "gain_setting",
    "read_speed_setting",
    "amp_read_area",
]


GHOST_ARMS = ["blue", "red", "slitv"]


class ArmFieldDispatcher:
    def __init__(self):
        # For now, coding for GHOST but I could imagine this being generified/subclassed for IGRINS2/etc
        # arguably I could code to the constants above, but this makes the subclass option cleaner when it comes
        self.arms = GHOST_ARMS
        self.descriptors = ((k, _conversion_functions.get(k, lambda x: x)) for k in GHOST_ARM_DESCRIPTORS)

    def populate_arm_fields(self):
        """
        Iterate over arm fields and parse out arm specific values.
        """
        # BE CAREFUL if you change this - the source and destination
        # may be the same.  So do not set_field on the field you are
        # reading as a dict unless you have already read out a copy

        for descriptor, value_munger in self.descriptors:

            # read field and keep a copy, per comment above
            val = self.get_field(descriptor)

            # default plain descriptor to None, will be set per arm, or via a bundle function after
            self.set_field(descriptor, None)
            if self.arm() is None:
                if val is None:
                    print(f"No return for {descriptor}")
                for arm in self.arms:
                    self.set_field(f"{descriptor}_{arm}", value_munger(val.get(arm, None)))
            else:  # val will be a value, not a dict
                self.set_field(f"{descriptor}_{self.arm()}", value_munger(val))
                self.set_field(descriptor, value_munger(val))

        # If this is a bundle, set any descriptors that require custom handling
        if self.arm() is None:
            for k, fn in _bundle_functions.items():
                val = self.get_field(k)
                self.set_field(k, fn(val))

    @abstractmethod
    def arm(self):
        return None

    @abstractmethod
    def get_field(self, field_name):
        return None

    @abstractmethod
    def set_field(self, field_name, value):
        pass


class ADInstrumentORMFieldDispatcher(ArmFieldDispatcher):
    def __init__(self, ad, instr):
        super().__init__()
        self.ad = ad
        self.instr = instr

    def arm(self):
        return self.ad.arm()

    def get_field(self, field_name):
        descr = getattr(self.ad, field_name, None)
        if descr is not None:
            return descr()
        return None

    def set_field(self, field_name, value):
        setattr(self.instr, field_name, value)


class Ghost(Base):
    """
    This is the ORM object for the GHOST details.
    """
    __tablename__ = 'ghost'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    header = relation(Header, order_by=id)
    arm = Column(Text, index=True)
    want_before_arc = Column(Boolean)
    disperser = Column(Text, index=True)
    filter_name = Column(Text, index=True)
    detector_name = Column(Text, index=True)
    detector_x_bin = Column(Integer, index=True)
    detector_y_bin = Column(Integer, index=True)
    amp_read_area = Column(Text, index=True)
    read_speed_setting = Column(READ_SPEED_SETTING_ENUM, index=True)
    gain_setting = Column(GAIN_SETTING_ENUM, index=True)
    res_mode = Column(RESOLUTION_ENUM, index=True)
    focal_plane_mask = Column(Text, index=True)
    prepared = Column(Boolean, index=True)
    overscan_subtracted = Column(Boolean, index=True)
    overscan_trimmed = Column(Boolean, index=True)

    detector_name_blue = Column(Text, index=True)
    detector_name_red = Column(Text, index=True)
    detector_name_slitv = Column(Text, index=True)
    detector_x_bin_blue = Column(Integer, index=True)
    detector_x_bin_red = Column(Integer, index=True)
    detector_x_bin_slitv = Column(Integer, index=True)
    detector_y_bin_blue = Column(Integer, index=True)
    detector_y_bin_red = Column(Integer, index=True)
    detector_y_bin_slitv = Column(Integer, index=True)
    exposure_time_blue = Column(Numeric(precision=8, scale=4))
    exposure_time_red = Column(Numeric(precision=8, scale=4))
    exposure_time_slitv = Column(Numeric(precision=8, scale=4))
    gain_setting_blue = Column(GAIN_SETTING_ENUM, index=True)
    gain_setting_red = Column(GAIN_SETTING_ENUM, index=True)
    gain_setting_slitv = Column(GAIN_SETTING_ENUM, index=True)
    read_speed_setting_blue = Column(READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_red = Column(READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_slitv = Column(READ_SPEED_SETTING_ENUM, index=True)
    amp_read_area_blue = Column(Text, index=True)
    amp_read_area_red = Column(Text, index=True)
    amp_read_area_slitv = Column(Text, index=True)

    def __init__(self, header, ad):
        """
        Create a Ghost instrument record

        Parameters
        ----------
        header : :class:`~header.Header`
            Corresponding header for the observation
        ad : :class:`astrodata.AstroData`
            Astrodata object to load Ghost information from
        """
        self.header = header

        # Populate from the astrodata object
        self.populate(ad)

    def populate(self, ad):
        """
        Populate the Ghost information from the given :class:`astrodata.AstroData`

        Parameters
        ----------
        ad : :class:`astrodata.Astrodata`
            Astrodata object to read Ghost information from
        """
        self.arm = ad.arm()
        self.want_before_arc = ad.want_before_arc()
        self.disperser = ad.disperser()
        self.filter_name = ad.filter_name()

        # Protect the database from field overflow from junk.
        # The datatype is precision=8, scale=4

        arm_field_dispatcher = ADInstrumentORMFieldDispatcher(ad, self)
        arm_field_dispatcher.populate_arm_fields()

        resolution = ad.res_mode()
        if resolution in RESOLUTIONS:
            self.res_mode = resolution

        self.focal_plane_mask = ad.focal_plane_mask()
        self.prepared = 'PREPARED' in ad.tags
        self.overscan_trimmed = 'OVERSCAN_TRIMMED' in ad.tags
        self.overscan_subtracted = 'OVERSCAN_SUBTRACTED' in ad.tags
