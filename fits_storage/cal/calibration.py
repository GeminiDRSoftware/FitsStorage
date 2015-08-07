"""
This module holds the Calibration superclass
"""

import functools
from ..orm.diskfile import DiskFile
from ..orm.header import Header

# A common theme across calibrations is that some of them don't handle processed data
# and will just return an empty list of calibs. This decorator is just some syntactic
# sugar for that common pattern.
def not_processed(f):
    @functools.wraps(f)
    def wrapper(self, processed=False, *args, **kw):
        if processed:
            return []
        return f(self, processed=processed, *args, **kw)

    return wrapper

# Another common pattern: calibrations that don't apply for imaging
def not_imaging(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kw):
        if self.descriptors['spectroscopy'] == False:
            return []
        return f(self, *args, **kw)

    return wrapper

# Another common pattern: calibrations that don't apply for spectroscopy
def not_spectroscopy(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kw):
        if self.descriptors['spectroscopy'] == True:
            return []
        return f(self, *args, **kw)

    return wrapper

class Calibration(object):
    """
    This class provides a basic Calibration Manager
    This is the superclass from which the instrument specific variants subclass
    """

    session = None
    header = None
    descriptors = None
    types = None
    applicable = []

    def __init__(self, session, header, descriptors, types):
        """
        Initialize a calibration manager for a given header object (ie data file)
        Need to pass in an sqlalchemy session that should already be open, this class will not close it
        Also pass in a header object
        """
        self.session = session
        self.header = header
        self.descriptors = descriptors
        self.types = types
        self.from_descriptors = False

        # Populate the descriptors dictionary for header
        if self.descriptors is None:
            self.from_descriptors = True
            self.types = eval(self.header.types)
            self.descriptors = {
                'header_id':            self.header.id,
                'observation_type':     self.header.observation_type,
                'observation_class':    self.header.observation_class,
                'spectroscopy':         self.header.spectroscopy,
                'object':               self.header.object,
                'instrument':           self.header.instrument,
                'central_wavelength':   self.header.central_wavelength,
                'program_id':           self.header.program_id,
                'ut_datetime':          self.header.ut_datetime,
                'exposure_time':        self.header.exposure_time,
                'observation_class':    self.header.observation_class,
                'detector_roi_setting': self.header.detector_roi_setting,
                'reduction':            self.header.reduction,
                'elevation':            self.header.elevation,
                'cass_rotator_pa':      self.header.cass_rotator_pa,
                'gcal_lamp':            self.header.gcal_lamp
                }
        else:
            # The data_section comes over as a native python array, needs to be a string
            if self.descriptors['data_section']:
                self.descriptors['data_section'] = str(self.descriptors['data_section'])

    def set_common_cals_filter(self, query, max_interval, limit):
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval

        return (query.filter(DiskFile.canonical == True)       # Search only canonical entries
                     .filter(Header.qa_state != 'Fail')        # Knock out the FAILs
                     .filter(Header.ut_datetime > datetime_lo) # Absolute time separation
                     .filter(Header.ut_datetime < datetime_hi)
                     .limit(limit))

    def bias(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def dark(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def flat(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def arc(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def fringe(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def ronchi_mask(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def pinhole_mask(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def spectwilight(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def lampoff_flat(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def domeflat(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def processed_fringe(self, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def specphot(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def photometric_standard(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def qh_flat(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def telluric_standard(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def polarization_standard(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def astrometric_standard(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def polarization_flat(self, processed=False, howmany=None):

        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []
