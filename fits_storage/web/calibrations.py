"""
This module contains the calibrations html generator function.
"""
import datetime
from ..orm import sessionfactory
from .selection import sayselection, queryselection, openquery
from ..cal import get_cal_object
from ..apache_return_codes import HTTP_OK
from ..fits_storage_config import fits_system_status

from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File

from . import templating

from sqlalchemy import join, desc

class Result(object):
    def __init__(self, **kw):
        for key, value in kw.items():
            setattr(self, key, value)

class WrappedCals(object):
    def __init__(self, applicable):
        self._applic    = applicable
        self._found     = False
        self._contents  = []

    def __iter__(self):
        for c in self._contents:
            yield c

    @property
    def found(self):
        return self._found

    @property
    def applicable(self):
        return self._applic

class WrapperObject(object):
    def __init__(self, session, header, counter, caloption, caltype):
        self.s         = session
        self.header    = header
        self._counter  = counter
        self._copt     = caloption
        self._ctype    = caltype
        self._takenow  = False
        self._warning  = False
        self._missing  = False
        self._requires = False
        self.c         = get_cal_object(session, None, header=header)
        self.cals      = {}

        self.process_cals()

    def process_cals(self):
        self.cals['arcs'] = self.arcs()
        self.cals['darks'] = self.darks()
        self.cals['biases'] = self.common('bias', self.c.bias)
        self.cals['flats'] = self.common('flat', self.c.flat)
        self.cals['pinhole_masks'] = self.common('pinhole_mask', self.c.pinhole_mask)
        self.cals['ronchi_masks'] = self.common('ronchi_mask', self.c.ronchi_mask)
        if self._warning:
            self._counter['warnings'] += 1
        if self._missing:
            self._counter['missings'] += 1

    @property
    def will_render(self):
        return (   (not self._copt)
                or (self._copt == 'warnings' and self._warning)
                or (self._copt == 'missing' and self._missing)
                or (self._copt == 'requires' and self._requires)
                or (self._copt == 'takenow' and self._takenow)   )

    @property
    def filename(self):
        return self.header.diskfile.file.name

    @property
    def datalabel(self):
        return self.header.data_label

    def arcs(self):
        if 'arc' in self.c.applicable and (self._ctype == 'all' or self._ctype == 'arc'):
            self._requires = True
            wrap = WrappedCals(applicable=True)

            # Look for an arc. Note no longer any requirement to support "sameprog" with the new archive
            arcs = self.c.arc()
            if arcs:
                wrap._found = True
                for arc in arcs:
                    r = Result(name  = arc.diskfile.file.name,
                               dl    = arc.data_label,
                               inter = None,
                               iwarn = False)
                    if arc.ut_datetime and self.header.ut_datetime:
                        r.inter = interval_string(arc, self.header)
                        if abs(interval_hours(arc, self.header)) > 24:
                            r.iwarn = True
                            self._warning = True
                    else:
                        self._warning = True

                    wrap._contents.append(r)
            else:
                self._warning = True
                self._missing = True

            # Handle the 'takenow' flag. This should get set to true if
            # no arc exists or
            # all the arcs generate warnings, and
            # the time difference between 'now' and the science frame is
            # less than the time difference between the science frame and the closest
            # arc to it that we currently have
            if self._missing:
                self._takenow = True
            elif self._warning:
                # Is it worth re-taking?
                # Find the smallest interval between a valid arc and the science
                newinterval = abs(interval_hours(arc, self.header))
                smallestinterval = newinterval
                # Is the smallest interval larger than the interval between now and the science?
                now = datetime.datetime.utcnow()
                then = self.header.ut_datetime
                nowinterval = now - then
                nowhours = (nowinterval.days * 24.0) + (nowinterval.seconds / 3600.0)
                if smallestinterval > nowhours:
                    self._takenow = True
        else:
            wrap = WrappedCals(applicable=False)

        return wrap

    def darks(self):
        if 'dark' in self.c.applicable and self._ctype in ('all', 'dark'):
            self._requires = True
            wrap = WrappedCals(applicable=True)

            darks = self.c.dark()
            if darks:
                wrap._found = True
                for dark in darks:
                    r = Result(name  = dark.diskfile.file.name,
                               dl    = dark.data_label,
                               inter = None,
                               iwarn = False)
                    if dark.ut_datetime and self.header.ut_datetime:
                        r.inter = interval_string(dark, self.header)
                        # GMOS darks can be up to 6 months away, others regular 5 day warning
                        hours_warn = 120
                        text_warn = '5 days'
                        if self.c.header.instrument in ['GMOS-N', 'GMOS-S']:
                            hours_warn = 4320
                            text_warn = '6 months'
                        if abs(interval_hours(dark, self.header)) > hours_warn:
                            r.warning = text_warn
                            r.iwarn = True
                            self._warning = True

                    wrap._contents.append(r)
            else:
                self._warning = True
                self._missing = True
        else:
            wrap = WrappedCals(applicable=False)

        return wrap

    def common(self, name, queryfn):
        if name in self.c.applicable and self._ctype in ('all', name):
            self._requires = True
            wrap = WrappedCals(applicable=True)

            cals = queryfn()
            if cals:
                for cal in cals:
                    r = Result(name  = cal.diskfile.file.name,
                               dl    = cal.data_label)
                    wrap._contents.append(r)
            else:
                self._warning = True
                self._missing = True
        else:
            wrap = WrappedCals(applicable=False)

        return wrap

# NOTE: As of September 2015 (when the code was refactored for templates), the following comment about not
#       showing processed calibrations still applies
#
#            # For now (March 2013) we don't want the SOS calibrations page to whine about processed cals at all.
#            #if('processed_bias' in c.applicable and (caltype=='all' or caltype=='processed_bias')):
#                #requires=True
#                #processed_bias = c.bias(processed=True)
#                #if(processed_bias):
#                    #html += "<H4>PROCESSED_BIAS: %s - %s</H4>" % (processed_bias.diskfile.file.name, processed_bias.data_label)
#                #else:
#                    #html += '<H3><FONT COLOR="Red">NO PROCESSED_BIAS FOUND!</FONT></H3>'
#                    #warning = True
#                    #missing = True
#
#            #if('processed_flat' in c.applicable and (caltype=='all' or caltype=='processed_flat')):
#                #requires=True
#                #processed_flat = c.flat(processed=True)
#                #if(processed_flat):
#                    #html += "<H4>PROCESSED_FLAT: %s - %s</H4>" % (processed_flat.diskfile.file.name, processed_flat.data_label)
#                #else:
#                    #html += '<H3><FONT COLOR="Red">NO PROCESSED_FLAT FOUND!</FONT></H3>'
#                    #warning = True
#                    #missing = True
#
#            #if('processed_fringe' in c.applicable and (caltype=='all' or caltype=='processed_fringe')):
#                #requires=True
#                #processed_fringe = c.processed_fringe()
#                #if(processed_fringe):
#                    #html += "<H4>PROCESSED_FRINGE: %s - %s</H4>" % (
#                                #processed_fringe.diskfile.file.name, processed_fringe.data_label)
#                #else:
#                    #html += '<H3><FONT COLOR="Red">NO PROCESSED_FRINGE FOUND!</FONT></H3>'
#                    #warning = True
#                    #missing = True

@templating.templated("calibrations.html", with_session = True, with_generator=True)
def calibrations(session, req, selection):
    """
    This is the calibrations generator. It implements a human readable calibration association server.
    This is mostly used by the Gemini SOSs to detect missing calibrations, and it defaults to the 
    SOS required calibrations policy.

    req is an apache request handler request object
    selection is an array of items to select on, simply passed
        through to the webhdrsummary function

    returns an apache request status code
    """
    counter = {
        'warnings': 0,
        'missings': 0,
    }

    template_args = dict(
        say_selection  = sayselection(selection),
        is_development = fits_system_status == "development",
        counter        = counter,
        )

    caloption = selection.get('caloption', '')
    session = sessionfactory()

    # OK, find the target files
    # The Basic Query
    query = session.query(Header).select_from(join(join(DiskFile, File), Header))

    # Only the canonical versions
    selection['canonical'] = True

    query = queryselection(query, selection)

    # If openquery, decline to do it
    if openquery(selection):
        template_args['is_open'] = True
        return template_args

    # Knock out the FAILs
    # Knock out ENG programs
    # Disregard SV-101. This is an undesirable hardwire
    # Order by date, most recent first
    headers = query.filter(Header.qa_state != 'Fail')\
                   .filter(Header.engineering != True)\
                   .filter(~Header.program_id.like('%SV-101%'))\
                   .order_by(desc(Header.ut_datetime))

    template_args['ndatasets'] = headers.count()

    # Was the request for only one type of calibration?
    caltype = 'all'
    if 'caltype' in selection:
        caltype = selection['caltype']

    template_args['objects'] = (WrapperObject(session, obj, counter, caloption, caltype) for obj in headers)

    return template_args

def interval_string(a, b):
    """
    Given two header objects, return a human readable string describing the time difference between them
    """
    t = interval_hours(a, b)

    word = "after"
    unit = "hours"

    if t < 0.0:
        word = "before"
        t *= -1.0

    if t > 48.0:
        t /= 24.0
        unit = "days"

    if t < 1.0:
        t *= 60
        unit = "minutes"

    # 1.2 days after
    string = "%.1f %s %s" % (t, unit, word)

    return string

def interval_hours(a, b):
    """
    Given two header objects, returns the number of hours b was taken after a
    """
    interval = a.ut_datetime - b.ut_datetime
    tdelta = (interval.days * 24.0) + (interval.seconds / 3600.0)

    return tdelta
