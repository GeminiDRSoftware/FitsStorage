"""
This module contains the calibrations html generator function.
"""
import datetime
from sqlalchemy import join, desc

from fits_storage.gemini_metadata_utils import gemini_date
from .selection import sayselection, queryselection, openquery, selection_to_URL
from fits_storage.cal.calibration import get_cal_object

from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File

from fits_storage.server.wsgi.context import get_context

from . import templating

from fits_storage.config import get_config


class Result(object):
    def __init__(self, **kw):
        for key, value in list(kw.items()):
            setattr(self, key, value)


class WrappedCals(object):
    def __init__(self, applicable):
        self._applic    = applicable
        self._contents  = []

    def __iter__(self):
        for c in self._contents:
            yield c

    def append(self, cal):
        self._contents.append(cal)

    @property
    def found(self):
        return len(self._contents) > 0

    @property
    def applicable(self):
        return self._applic


class WrapperObject(object):
    def __init__(self, header, counter, caloption, caltype):
        self.header    = header
        self._counter  = counter
        self._copt     = caloption
        self._ctype    = caltype
        self._takenow  = False
        self._warning  = False
        self._missing  = False
        self._requires = False
        self.c         = get_cal_object(get_context().session, None, header=header)
        self.cals      = {}

        self.process_cals()

    def process_cals(self):
        self.cals['arcs'] = self.arcs()
        self.cals['darks'] = self.darks()
        self.cals['biases'] = self.common('bias', self.c.bias)
        self.cals['flats'] = self.flats()
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
    def showing_missing(self):
        return self._copt == 'missing' and self._missing

    @property
    def filename(self):
        return self.header.diskfile.file.name

    @property
    def datalabel(self):
        return self.header.data_label

    def arcs(self):
        if (self.c.header.instrument not in ['GMOS-N', 'GMOS-S'] or self.c.header.observation_class != 'dayCal') \
                and 'arc' in self.c.applicable and (self._ctype == 'all' or self._ctype == 'arc'):
            self._requires = True
            wrap = WrappedCals(applicable=True)

            # Look for an arc. Note no longer any requirement to support "sameprog" with the new archive
            arcs = self.c.arc()
            if arcs:
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

                    wrap.append(r)
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

    def flats(self):
        if self.c.header.instrument.startswith('GMOS') and "'IMAGE'" in self.c.header.types:
            return WrappedCals(applicable=False)
        else:
            return self.common('flat', self.c.flat)

    def darks(self):
        skip = False
        # per Jocelyn Ferrara and Adam Smith, GMOS images with MASK_NAME of focus_array_new can be skipped
        if self.c.header.instrument in ['GMOS-N', 'GMOS-S'] \
                and self.c.header.spectroscopy is False \
                and self.c.descriptors.get('focal_plane_mask', '') == 'focus_array_new':
            skip = True
        if not skip and 'dark' in self.c.applicable and self._ctype in ('all', 'dark'):
            self._requires = True
            wrap = WrappedCals(applicable=True)

            darks = self.c.dark()
            if darks:
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

                    wrap.append(r)
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
                    wrap.append(r)
            else:
                self._warning = True
                self._missing = True
        else:
            wrap = WrappedCals(applicable=False)

        return wrap


@templating.templated("calibrations.html", with_generator=True)
def calibrations(selection):
    """
    This is the calibrations generator. It implements a human-readable
    calibration association server. This is mostly used by the Gemini SOSs to
    detect missing calibrations, and it defaults to the SOS required
    calibrations policy.
    """
    fsc = get_config()
    counter = {
        'warnings': 0,
        'missings': 0,
    }

    template_args = dict(
        fits_server    = fsc.fits_server_name,
        secure         = fsc.is_archive,
        say_selection  = sayselection(selection),
        is_development = fsc.fits_system_status == "development",
        counter        = counter,
        )

    caloption = selection.get('caloption', '')

    # OK, find the target files
    # The Basic Query
    query = get_context().session.query(Header).select_from(join(join(DiskFile, File), Header))

    # Only the canonical versions
    selection['canonical'] = True

    query = queryselection(query, selection)

    try:
        if 'date' in selection:
            dt = selection['date']
            dt = datetime.datetime.strptime(dt, '%Y%m%d')
            nextdt = dt + datetime.timedelta(days=1)
            nextdt = nextdt.strftime('%Y%m%d')
            if nextdt > gemini_date('today'):
                nextdt = None
            prevdt = dt - datetime.timedelta(days=1)
            prevdt = prevdt.strftime('%Y%m%d')

            if nextdt:
                nextsel = selection.copy()
                del nextsel['canonical']
                nextsel['date'] = nextdt
                template_args['next'] = f"&nbsp;|&nbsp;<a href=\"/calibrations{selection_to_URL(nextsel)}\">{nextdt}</a> &gt;&gt;"
            if prevdt:
                prevsel = selection.copy()
                del prevsel['canonical']
                prevsel['date'] = prevdt
                template_args['prev'] = f"&lt;&lt; <a href=\"/calibrations{selection_to_URL(prevsel)}\">{prevdt}</a>"
    except Exception as e:
        # best effort, we'll just exclude the links for prev/next day
        raise

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

    template_args['objects'] = (WrapperObject(obj, counter, caloption, caltype) for obj in headers)

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
