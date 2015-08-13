"""
This module holds the Calibration superclass
"""

import functools
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from sqlalchemy import func
from sqlalchemy.orm import join
from datetime import timedelta

from .. import gemini_metadata_utils as gmu

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

class CalQuery(object):
    def __init__(self, session, instrClass, descriptors):
        self.descr = descriptors
        self.query = (session.query(Header).select_from(join(join(instrClass, Header), DiskFile))
                                           .filter(DiskFile.canonical == True) # Search only canonical entries
                                           .filter(Header.qa_state != 'Fail')) # Knock out the FAILs

    def __call_through(self, query_method, *args, **kw):
        self.query = query_method(*args, **kw)
        return self

    def add_filters(self, *args):
        for arg in args:
            self.query = self.query.filter(arg)

        return self

    def match_descriptors(self, *args):
        for arg in args:
            field = arg.expression.name
            self.query = self.query.filter(arg == self.descr[field])

        return self

    def max_interval(self, **kw):
        max_int = timedelta(**kw)
        datetime_lo = self.descr['ut_datetime'] - max_int
        datetime_hi = self.descr['ut_datetime'] + max_int
        targ_ut_dt_secs = int((self.descr['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())

        self.query = (self.query.filter(Header.ut_datetime > datetime_lo) # Absolute time separation
                                .filter(Header.ut_datetime < datetime_hi)
                                .order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))) # Order by absolute time separation.

        return self

    def if_(self, condition, methodname, *args, **kw):
        if condition:
            return getattr(self, methodname)(*args, **kw)

        return self

    def __getattr__(self, name):
        try:
            return functools.partial(self.__call_through, getattr(self.query, name))
        except AttributeError:
            if name in gmu.obs_types:
                return functools.partial(self.observation_type, name)
            elif name in gmu.obs_classes:
                return functools.partial(self.observation_class, name)
            elif name in gmu.reduction_states:
                return functools.partial(self.reduction, name)

            raise AttributeError("'{}' object has no attribute '{}'".format(self.__class__.__name__, name))

    def all(self):
        return self.query.all()

    # The following add filters specific to certain types of calibs

    def tolerance(condition=True, **kw):
        if condition:
            for descriptor, tol in kw.items():
                # Occassionally we get a None for some descriptors, so run this in a try except
                try:
                    lo = float(self.descr[descriptor]) - tol
                    hi = float(self.descr[descriptor]) + tol
                    # This may raise AttributeError, we let it go through
                    column = getattr(Header, descriptor)
                    self.query = (self.query.filter(column > lo).filter(column < hi))
                except KeyError:
                    raise RuntimeError("No such descriptor '{}' defined in this instrument".format(descriptor))
                except TypeError:
                    pass

        return self

    def raw(self):
        return self.reduction('RAW')

    def reduction(self, red):
        self.query = self.query.filter(Header.reduction == red)
        return self

    def observation_type(self, ot):
        self.query = self.query.filter(Header.observation_type == ot)
        return self

    def observation_class(self, oc):
        self.query = self.query.filter(Header.observation_class == oc)
        return self

    def object(self, ob):
        self.query = self.query.filter(Header.object == ob)
        return self

    def spectroscopy(self, status):
        self.query = self.query.filter(Header.spectroscopy == status)
        return self

    def raw_or_processed(self, name, processed):
        if processed:
            return self.reduction('PROCESSED_' + name)
        else:
            return self.raw().observation_type(name)

    def bias(self, processed=False):
        return self.raw_or_processed('BIAS', processed)

    def dark(self, processed=False):
        return self.raw_or_processed('DARK', processed)

    def flat(self, processed=False):
        return self.raw_or_processed('FLAT', processed)

    def arc(self, processed=False):
        return self.raw_or_processed('ARC', processed)

    def pinhole(self, processed=False):
        return self.raw_or_processed('PINHOLE', processed)

    def photometric_standard(self, processed=False, **kw):
        if processed:
            # NOTE: PROCESSED_PHOTSTANDARDS are not used anywhere; this is an advance...
            return self.reduction('PROCESSED_PHOTSTANDARD')
        else:
            ret = self.raw().spectroscopy(False)
            for key in kw:
                ret = getattr(self, key)()
            return ret

    def telluric_standard(self, processed=False, **kw):
        if processed:
            return self.reduction('PROCESSED_TELLURIC')
        else:
            ret = self.raw().spectroscopy(True)
            for key in kw:
                ret = getattr(self, key)()
            return ret

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
    instrClass = None
    instrDescriptors = ()

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
                'detector_roi_setting': self.header.detector_roi_setting,
                'reduction':            self.header.reduction,
                'elevation':            self.header.elevation,
                'cass_rotator_pa':      self.header.cass_rotator_pa,
                'gcal_lamp':            self.header.gcal_lamp
                }

            iC = self.instrClass
            query = session.query(iC).filter(iC.header_id == self.descriptors['header_id'])
            inst = query.first()

            # Populate the descriptors dictionary for the instrument
            for descr in self.instrDescriptors:
                self.descriptors[descr] = getattr(inst, descr)
        else:
            # The data_section comes over as a native python array, needs to be a string
            if self.descriptors['data_section']:
                self.descriptors['data_section'] = str(self.descriptors['data_section'])

        # Set the list of applicable calibrations
        self.set_applicable()

    def get_query(self):
        return CalQuery(self.session, self.instrClass, self.descriptors)

    def set_common_cals_filter(self, query, max_interval, limit):
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())

        return (query.filter(DiskFile.canonical == True)       # Search only canonical entries
                     .filter(Header.qa_state != 'Fail')        # Knock out the FAILs
                     .filter(Header.ut_datetime > datetime_lo) # Absolute time separation
                     .filter(Header.ut_datetime < datetime_hi)
                     .order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs)) # Order by absolute time separation.
                     .limit(limit))

    def set_applicable(self):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        self.applicable = []

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
