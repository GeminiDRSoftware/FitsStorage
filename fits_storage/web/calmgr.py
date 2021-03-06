"""
This module contains the calmgr html generator function.
"""
from collections import namedtuple

import simplejson as json

from fits_storage.utils.api import WSGIError, BAD_REQUEST

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from .selection import queryselection, openquery

from ..utils.web import get_context, Return

from ..cal import get_cal_object
from ..fits_storage_config import storage_root, fits_servername
from ..gemini_metadata_utils import cal_types

from . import templating
from .templating import SkipTemplateError

from sqlalchemy import join, desc

from psycopg2 import InternalError
from sqlalchemy.exc import DataError

import urllib.request, urllib.parse, urllib.error
import re
import os
import datetime
import sys
import traceback

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
                cals = getattr(cal_obj, method)(**args)
            except (InternalError, DataError):
                get_context().session.rollback()
                add_note("Rolling back errored transaction in cal query")
                get_context().session.commit()
                raise

            cal_res = []
            for cal in cals:
                if cal.diskfile.present:
                    if http:
                        url = "http://{host}/file/{name}".format(host=hostname,
                                                                 name=cal.diskfile.file.name)
                    else:
                        path = os.path.join(storage_root, cal.diskfile.file.path, cal.diskfile.file.name)
                        url = "file://{}".format(path)
                else:
                    # Once we are sending new stlye processed calibrations to the GSA,
                    # we can form a URL to the GSA here and return that.
                    url = None

                cal_res.append(
                    dict(label = cal.data_label,
                         name  = cal.diskfile.file.name,
                         md5   = cal.diskfile.data_md5,
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

def generate_post_calmgr(selection, caltype, procmode=None):
    # OK, get the details from the POST data
    ctx = get_context()
    clientdata = ctx.raw_data
    if clientdata:
        clientdata = clientdata.decode('utf-8', errors='ignore')
    clientstr = urllib.parse.unquote_plus(clientdata)

    match = re.match("descriptors=(.*)&types=(.*)", clientstr)
    desc_str = match.group(1)
    type_str = match.group(2)

    usagelog = ctx.usagelog
    usagelog.add_note("CalMGR request desc_str: %s" % desc_str)
    usagelog.add_note("CalMGR request type_str: %s" % type_str)
    try:
        desc_str = re.sub(r'\'dispersion_axis\': <map object at 0x.+>', '\'dispersion_axis\': None', desc_str)
        descriptors = eval(desc_str)
    except SyntaxError as sxe:
        raise SkipTemplateError(message="Invalid descriptors field in request: {}".format(desc_str),
                                content_type='text/plain', status = Return.HTTP_BAD_REQUEST)
    types = eval(type_str)
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
                             hostname=fits_servername,
                             storage_root=storage_root)
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
    query = queryselection(query, selection)\
                 .filter(Header.qa_state != 'Fail')\
                 .order_by(desc(Header.ut_datetime))

    # If openquery, limit number of responses
    # NOTE: This shouldn't happen, as we're disallowing open queries
    if openquery(selection):
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
    if method == 'GET' and openquery(selection):
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
