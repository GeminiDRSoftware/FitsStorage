"""
This module contains the calmgr html generator function.
"""
from collections import namedtuple
from sqlalchemy import join, desc

import simplejson as json

from psycopg2 import InternalError
from sqlalchemy.exc import DataError

import urllib.parse
import re
import os
import datetime
import sys
import traceback
import copy

from ast import literal_eval

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.db.selection import Selection

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.cal.calibration import get_cal_object

from fits_storage.gemini_metadata_utils import cal_types

from . import templating
from .templating import SkipTemplateError

from fits_storage.config import get_config

no_func = lambda x: None

NonLinCoeffs = lambda *args, **kwargs : ""

Section = namedtuple('Section', 'x1 x2 y1 y2')


args_for_cals = {
    # cal_type      : (method_name, {arg_name: value, ...})
    'processed_arc':  ('arc', {'processed': True}),
    'processed_bias': ('bias', {'processed': True}),
    'processed_dark': ('dark', {'processed': True}),
    'processed_flat': ('flat', {'processed': True}),
    'processed_standard': ('standard', {'processed': True}),
    'processed_bpm': ('bpm', {'processed': True}),
    'processed_pinhole': ('pinhole', {'processed': True}),
    'processed_telluric': ('telluric', {'processed': True})
    }


def cals_info(cal_obj, caltype, procmode=None, qtype='UNKNOWN', log=no_func, add_note=no_func, http=True, hostname=None, storage_root=''):
    resp = []

    # Figure out which caltype(s) we want
    if caltype == '':
        caltypes = cal_types
    else:
        caltypes = [caltype]

    # Go through the caltypes list
    for ct in caltypes:
        # Call the appropriate method depending what calibration type we want
        try:
            # if ct is one of the recognized cal_types, we'll invoke
            # the method in cal_obj with the same name as ct. If there's
            # no such name, like in the case of various processed_XXX, which
            # are obtained invoking other method with some arguments, then
            # we can introduce a mappint in args_for_cals
            method, args = args_for_cals.get(ct, (ct, {}))
            try:
                cals = getattr(cal_obj, method)(*args) \
                    if hasattr(cal_obj, method) else []
            except (InternalError, DataError):
                get_context().session.rollback()
                add_note("Rolling back errored transaction in cal query")
                get_context().session.commit()
                raise

            cal_res = []
            for cal in cals:
                if cal.diskfile.present:
                    if http:
                        urlpath = f"{cal.diskfile.path}/{cal.diskfile.filename}"\
                            if cal.diskfile.path else f"{cal.diskfile.filename}"
                        url = f"http://{hostname}/file/{urlpath}"
                    else:
                        path = os.path.join(storage_root, cal.diskfile.path, cal.diskfile.filename)
                        url = f"file://{path}"
                else:
                    # Once we are sending new stlye processed calibrations to the GSA,
                    # we can form a URL to the GSA here and return that.
                    url = None

                cal_res.append(
                    dict(label = cal.data_label,
                         name  = cal.diskfile.file.name,
                         md5   = cal.diskfile.file_md5,
                         url   = url
                    ))

                add_note("CalMGR %s returning %s %s" % (qtype, ct, cal.diskfile.file.name))

            yield dict(
                error = False,
                type  = ct,
                cals  = cal_res
                )

            if len(cals) == 0:
                add_note("CalMGR %s - no calibration type %s found" % (qtype, ct))
        except Exception:
            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            log("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
            add_note("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

            yield dict(
                error = True,
                type  = ct,
                cals  = None
                )


def _cal_eval(str):
    """
    Modified safe eval() for calibration requests.

    The
    """
    str = re.sub(r'NonLinCoeffs\([^\)]*\)', '""', str)
    str = re.sub(r'Section\( *x1=(\d+), *x2=(\d*), *y1=(\d*), *y2=(\d*)\)', '"[\\1, \\2, \\3, \\4]"', str)

    # handle datetime.datetime, datetime.date, datetime.time
    datetimes_dict = {}
    for typ, typfn in (('datetime', datetime.datetime),
                       ('date', datetime.date),
                       ('time', datetime.time)):
        rex = r'\'([\w\d]+)\': *(datetime.)?' + typ + r'\(([ \d,]+)\)'
        datetimes = re.findall(rex, str)
        for dt in datetimes:
            key = dt[0]
            args = dt[2]
            args = [int(a) for a in args.split(',')]
            val = typfn(*args)
            datetimes_dict[key] = val
    # now we need to remove those datetimes, as we'll get them from datetimes_dict
    # this gets any datetimes that have a , after them
    str = re.sub(r'\'([\w\d]+)\': *(datetime.)?(datetime|date|time)\(([ \d,]+)\),', '', str)
    # this gets any remaining datetimes that have a , before them or the single k:v datetime
    str = re.sub(r',? *\'([\w\d]+)\': *(datetime.)?(datetime|date|time)\(([ \d,]+)\)', '', str)
    retval = literal_eval(str)
    retval.update(datetimes_dict)
    return retval

def parse_post_calmgr_inputs(ctx):
    """
    Parse data posted to the calmgr.

    Pass the context rather than the clientdata directly, so that we can also
    access the usagelog, for example.

    Returns descriptor and types dictionaries.
    This function automatically handles the various (ie old style and json)
    data formats, and safely processes them as untrusted input
    """
    clientdata = ctx.raw_data
    usagelog = ctx.usagelog

    if clientdata is None:
        usagelog.add_note("Missing POST data")
        raise SkipTemplateError(
                message="Missing POST data",
                content_type='text/plain', status = Return.HTTP_BAD_REQUEST)

    # Handle "old style" data
    try:
        sequencedata = clientdata.decode('utf-8', errors='ignore')
        sequencedata = urllib.parse.unquote_plus(sequencedata)
    except:
        sequencedata = None
    match = re.match("descriptors=(.*)&types=(.*)", sequencedata)
    if match:
        try:
            desc_str = match.group(1)
            type_str = match.group(2)
        except ValueError:
            raise SkipTemplateError(
                message=f"Invalid post data format: {sequencedata}",
                content_type='text/plain', status = Return.HTTP_BAD_REQUEST)
        usagelog.add_note("CalMGR request desc_str: %s" % desc_str)
        usagelog.add_note("CalMGR request type_str: %s" % type_str)

        try:
            descriptors = _cal_eval(desc_str)
        except SyntaxError as sxe:
            raise SkipTemplateError(
                message=f"Invalid descriptors field in request: {desc_str}",
                content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

        try:
            types = literal_eval(type_str)
        except SyntaxError as sxe:
            raise SkipTemplateError(
                message=f"Invalid types field in request: {type_str}",
                content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

        usagelog.add_note("CalMGR request Descriptor Dictionary: %s" % descriptors)
        usagelog.add_note("CalMGR request Types List: %s" % types)

        return (descriptors, types)

    # If it wasn't old style, it must be JSON
    try:
        payload = json.loads(clientdata)
    except json.JSONDecodeError:
        usagelog.add_note(f"JSONDecode Error: {clientdata}")
        raise SkipTemplateError(
            message=f"Unable to parse JSON POST data.",
            content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

    if not isinstance(payload, dict):
        usagelog.add_note(f"Malformed JSON POST data - value is not a dict")
        raise SkipTemplateError(
            message=f"Malformed JSON POST data - value is not a dict.",
            content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

    types = payload.get("tags")
    if not isinstance(types, list):
        usagelog.add_note(f"Malformed JSON POST data - "
                          f"missing or malformed tags item")
        raise SkipTemplateError(
            message=f"Malformed JSON POST data - missing or malformed tags item",
            content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

    descriptors = payload.get("descriptors")
    if not isinstance(descriptors, dict):
        usagelog.add_note(f"Malformed JSON POST data - "
                          f"missing or malformed descriptors item")
        raise SkipTemplateError(
            message=f"Malformed JSON POST data - missing or malformed descriptors item",
            content_type='text/plain', status=Return.HTTP_BAD_REQUEST)

    # Do type conversions for datetimes
    datetimes = ['ut_datetime']
    for dt in datetimes:
        if dt in descriptors:
            descriptors[dt] = datetime.datetime.fromisoformat(descriptors[dt])

    return descriptors, types


def generate_post_calmgr(selection, caltype, procmode=None):
    fsc = get_config()
    ctx = get_context()
    usagelog = ctx.usagelog

    # Get the details from the POST data
    descriptors, types = parse_post_calmgr_inputs(ctx)

    usagelog.add_note("CalMGR request CalType: %s" % caltype)
    usagelog.add_note("CalMGR request Descriptor Dictionary: %s" % descriptors)
    usagelog.add_note("CalMGR request Types List: %s" % types)

    # commit these now in case anything goes wrong later
    ctx.session.commit()

    # OK, there are a couple of items that are handled in the DB as if they are descriptors
    # but they're actually types. This is where we push them into the descriptor disctionary
    descriptors['nodandshuffle'] = 'NODANDSHUFFLE' in types
    descriptors['spectroscopy'] = 'SPECT' in types
    descriptors['overscan_subtracted'] = 'OVERSCAN_SUBTRACTED' in types
    descriptors['overscan_trimmed'] = 'OVERSCAN_TRIMMED' in types
    descriptors['prepared'] = 'PREPARED' in types

    # Get a cal object for this target data
    try:
        c = get_cal_object(ctx.session, None, header=None, procmode=procmode, descriptors=descriptors, types=types)
    except KeyError as ke:
        raise SkipTemplateError(message="Missing field in request: {}".format(ke), content_type='text/plain', status = Return.HTTP_BAD_REQUEST)

    yield dict(
        label    = descriptors['data_label'],
        filename = None,
        md5      = None,
        cal_info = cals_info(c, caltype,
                             qtype='POST',
                             log=ctx.log,
                             add_note=usagelog.add_note,
                             hostname=fsc.fits_server_name,
                             storage_root=fsc.storage_root)
        )

    # Commit the changes to the usagelog
    ctx.session.commit()

def generate_get_calmgr(selection, caltype, procmode=None):
    # OK, we got called via a GET - find the science datasets in the database
    # The Basic Query

    ctx = get_context()
    session = ctx.session

    query = session.query(Header).select_from(join(join(File, DiskFile), Header))

    # Query the selection
    # Knock out the FAILs
    # Order by date, most recent first
    query = selection.filter(query)\
                 .filter(Header.qa_state != 'Fail')\
                 .order_by(desc(Header.ut_datetime))

    # If openquery, limit number of responses
    # NOTE: This shouldn't happen, as we're disallowing open queries
    if selection.openquery:
        query = query.limit(1000)

    # OK, do the query
    headers = query.all()

    usagelog = ctx.usagelog
    # Did we get anything?
    if len(headers) > 0:
        # Loop through targets frames we found
        for header in headers:
            # Get a cal object for this target data
            c = get_cal_object(ctx.session, None, header=header, procmode=procmode)

            yield dict(
                label    = header.data_label,
                filename = header.diskfile.file.name,
                md5      = header.diskfile.data_md5,
                cal_info = cals_info(c, caltype,
                                     qtype='GET',
                                     log=ctx.log,
                                     add_note=usagelog.add_note,
                                     hostname=ctx.env.server_hostname),
                )

    # commit the usagelog notes
    session.commit()


def jsoncalmgr(selection):
    dat = calmgr(selection)
    get_context().resp.append(json.dumps(dat['generator'], indent=4, iterable_as_array=True))


@templating.templated("calmgr.xml", content_type='text/xml')
def xmlcalmgr(selection):
    return calmgr(selection)


def calmgr(selection):
    """
    This is the calibration manager. It implements a machine readable calibration association server
    type is the summary type required
    selection is an array of items to select on, simply passed through to the webhdrsummary function
    - in this case, this will usually be a datalabel or filename

    if this code is called via an HTTP POST request rather than a GET, it expects to
    receive a string representation of a python dictionary containing descriptor values
    and a string representation of a python array containg astrodata types
    and it will use this data as the science target details with which to associate
    the calibration.

    This uses the calibration classes to do the association. It doesn't reference the
    "applicable" feature of the calibration classes though, it attempts to find a calibration
    of the type requested regardless of its applicability.
    """

    ctx = get_context()
    method = ctx.env.method

    # There selection has to be a closed query for a GET. If it's open, then disallow
    if method == 'GET' and selection.openquery:
        ctx.usagelog.add_note("Error: Selection cannot represent an open query for calibration association")
        ctx.resp.client_error(Return.HTTP_NOT_ACCEPTABLE, content_type='text/plain',
                                    message='<!-- Error: Selection cannot represent an open query for calibration association -->\n\n')

    # Only the canonical versions
    selection['canonical'] = True

    # Was the request for only one type of calibration?
    caltype = selection.get('caltype', '')
    procmode = selection.get('procmode', None)

    # An empty cal type is acceptable for GET - means to list all the calibrations available
    if not caltype and method == 'POST':
        ctx.usagelog.add_note("Error: No calibration type specified")
        ctx.resp.client_error(Return.HTTP_METHOD_NOT_ALLOWED, content_type='text/plain', message='<!-- Error: No calibration type specified-->\n\n')

    gen = (generate_post_calmgr if method == 'POST' else generate_get_calmgr)

    return dict(
        machine_name = os.uname()[1],
        req_method   = method,
        now          = datetime.datetime.now(),
        utcnow       = datetime.datetime.utcnow(),
        generator    = gen(selection, caltype, procmode),
        )
