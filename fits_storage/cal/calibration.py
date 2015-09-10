"""
This module holds the Calibration superclass
"""

import functools
from ..orm.file     import File
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
    """Class that helps to build the queries for calibration.

       Using CalQuery reduces a great deal of the boilerplate code and provides a
       kind of DSL (Domain Specific Language) that should make query building more
       natural -or easier to understand- for the calibrations."""

    def __init__(self, session, instrClass, descriptors, full_query=False):
        # Keep a copy of the instrument descriptors and start the query with
        # some common filters
        self.descr = descriptors
        if full_query:
            query = (session.query(Header, DiskFile, File)
                            .select_from(join(join(join(instrClass, Header), DiskFile), File))
                            .filter(DiskFile.id == Header.diskfile_id)
                            .filter(File.id == DiskFile.file_id))
        else:
            query = (session.query(Header)
                            .select_from(join(join(instrClass, Header), DiskFile)))
        self.query = (query.filter(DiskFile.canonical == True) # Search only canonical entries
                           .filter(Header.qa_state != 'Fail')) # Knock out the FAILs

    def __call_through(self, query_method, *args, **kw):
        "Used to make arbitrary calls to the internal SQLAlchemy query object"
        self.query = query_method(*args, **kw)
        return self

    def add_filters(self, *args):
        """Takes a number of arguments (`args`) which are SQLAlchemy expressions.

           Adds each one of those expressions to the internal query as filters, effectively
           adding conditions like this: `args[0] AND args[1] AND ...`"""
        for arg in args:
            self.query = self.query.filter(arg)

        return self

    def match_descriptors(self, *args):
        """In our usual queries, there are plenty of simple filters like this:

              Header.foo == descriptors['foo']
              Instrument.bar == descriptors['bar']

           `match_descriptors` takes a numbers of expressions (eg. `Header.foo`, `Instrument.bar`),
           figures out the descriptor to use from the column name, and adds the right filter to
           the query."""
        for arg in args:
            field = arg.expression.name
            self.query = self.query.filter(arg == self.descr[field])

        return self

    def max_interval(self, **kw):
        """Max interval requires keyword arguments. The arguments it accepts are the same as
           `datetime.timedelta` (`days`, `seconds`, ...)

           Using those arguments, it will construct a timedelta and a filter will be added
           that constrains the returned calibrations so that their datetimes fit in:

                header.ut_datetime-delta < cal.ut_datetime < header.ut_datetime+delta"""
        max_int = timedelta(**kw)
        datetime_lo = self.descr['ut_datetime'] - max_int
        datetime_hi = self.descr['ut_datetime'] + max_int

        self.query = (self.query.filter(Header.ut_datetime > datetime_lo) # Absolute time separation
                                .filter(Header.ut_datetime < datetime_hi))

        return self

    def if_(self, condition, methodname, *args, **kw):
        """A convenience for filters that are added conditionally. To be used like in this example
           from GMOS:

             if_(self.descriptors['nodandshuffle'], 'match_descriptors', Gmos.nod_count, Gmos.nod_pixels)

           This method exists only so that we can construct a whole query just by chaining."""
        if condition:
            return getattr(self, methodname)(*args, **kw)

        return self

    def __getattr__(self, name):
        """The CalQuery instances define only a handful of methods that are used often, but we would
           still want to expose other elements. Instead of creating methods for them, we try to figure
           out what the user wanted from the name of the attribute. The attribute search follows this
           order, returning the first that matches:

             1) See if the the internal SQLAlchemy query has that attribute. In this way we allow
                arbitrary queries to be formed, but this shouldn't be abused.
             2) See if the attribute is one of the observation types. If it is, then invoking the
                returned object will be the same as doing `.observation_type('attribute_name')`
             3) Same as 2), but with observation classes and the `.observation_class` method
             4) Same as 2), but with reduction states and the `.reduction` method
             5) If nothing matches, raise an AttributeError"""
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

    def all(self, limit, default_order = True):
        """Returns a list of results, limited in number by the `limit` argument.

           If `default_order` is `True` (the default), then the results are sorted by absolute
           time separation. If it's `False`, no ordering will be performed; if the user wanted some sorting,
           it has to be applied *before* invoking this method."""
        if default_order:
            # Order by absolute time separation.
            targ_ut_dt_secs = int((self.descr['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
            self.query = self.query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))
        return self.query.limit(limit).all()

    # The following add filters specific to certain types of calibs

    def tolerance(self, condition=True, **kw):
        """Takes a number of descriptors as keyword arguments (only valid for the common header, not
           the instrument specific descriptors), in the form `descriptor=tolerance`.

           The internal query will get added filters for each descriptor to ensure that the results
           match within the specified tolerance.

           There's an extra keyword argument, `condition`: if condition is false, the tolerances
           won't be added (by default it's true). This is so that we can add or skip the tolerance
           tests to the query using just call chaining, to keep everything compact."""
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
                    raise RuntimeError("No such descriptor '{}' defined in the header".format(descriptor))
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

    def __init__(self, session, header, descriptors, types, full_query=False):
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
        self.full_query = full_query

        # Populate the descriptors dictionary for header
        if self.descriptors is None and self.instrClass is not None:
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
                'coadds':               self.header.coadds,
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
        elif self.descriptors is not None:
            # The data_section comes over as a native python array, needs to be a string
            if self.descriptors['data_section']:
                self.descriptors['data_section'] = str(self.descriptors['data_section'])

        # Set the list of applicable calibrations
        self.set_applicable()

    def get_query(self):
        return CalQuery(self.session, self.instrClass, self.descriptors, full_query=self.full_query)

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
