"""
This module contains the calmgr html generator function.
"""
from ..orm import sessionfactory
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from .selection import queryselection, openquery

from ..utils.web import Context

from ..cal import get_cal_object
from ..fits_storage_config import using_apache, storage_root, fits_servername
from ..gemini_metadata_utils import cal_types
from ..apache_return_codes import HTTP_OK, HTTP_NOT_ACCEPTABLE

from . import templating

from sqlalchemy import join, desc

import urllib
import re
import os
import datetime
import sys
import traceback

no_func = lambda x: None

args_for_cals = {
    # cal_type      : (method_name, {arg_name: value, ...})
    'processed_arc':  ('arc', {'processed': True}),
    'processed_bias': ('bias', {'processed': True}),
    'processed_dark': ('dark', {'processed': True}),
    'processed_flat': ('flat', {'processed': True})
    }

def cals_info(cal_obj, caltype, qtype='UNKNOWN', log=no_func, add_note=no_func, http=True, hostname=None, storage_root=''):
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
            if ct in cal_types:
                # if ct is one of the recognized cal_types, we'll invoke
                # the method in cal_obj with the same name as ct. If there's
                # no such name, like in the case of various processed_XXX, which
                # are obtained invoking other method with some arguments, then
                # we can introduce a mappint in args_for_cals
                method, args = args_for_cals.get(ct, (ct, {}))
                cals = getattr(cal_obj, method)(**args)
            else:
                cals = []

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
            yield dict(
                error = True,
                type  = ct,
                cals  = None
                )

            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            log("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
            add_note("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

def generate_post_calmgr(session, req, selection, caltype):
    # OK, get the details from the POST data
    ctx = Context()
    clientdata = ctx.req.raw_data
    clientstr = urllib.unquote_plus(clientdata)

    match = re.match("descriptors=(.*)&types=(.*)", clientstr)
    desc_str = match.group(1)
    type_str = match.group(2)

    descriptors = eval(desc_str)
    types = eval(type_str)
    usagelog = ctx.usagelog
    usagelog.add_note("CalMGR request CalType: %s" % caltype)
    usagelog.add_note("CalMGR request Descriptor Dictionary: %s" % descriptors)
    usagelog.add_note("CalMGR request Types List: %s" % types)

    # OK, there are a couple of items that are handled in the DB as if they are descriptors
    # but they're actually types. This is where we push them into the descriptor disctionary
    descriptors['nodandshuffle'] = 'GMOS_NODANDSHUFFLE' in types
    descriptors['spectroscopy'] = 'SPECT' in types
    descriptors['overscan_subtracted'] = 'OVERSCAN_SUBTRACTED' in types
    descriptors['overscan_trimmed'] = 'OVERSCAN_TRIMMED' in types
    descriptors['prepared'] = 'PREPARED' in types

    # Get a cal object for this target data
    c = get_cal_object(session, None, header=None, descriptors=descriptors, types=types)

    yield dict(
        label    = descriptors['data_label'],
        filename = None,
        md5      = None,
        cal_info = cals_info(c, caltype, qtype='POST',
                                      log=ctx.req.log,
                                      add_note=usagelog.add_note,
                                      hostname=fits_servername,
                                      storage_root=storage_root)
        )

    # Commit the changes to the usagelog
    session.commit()

def generate_get_calmgr(session, req, selection, caltype):
    # OK, we got called via a GET - find the science datasets in the database
    # The Basic Query
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

    ctx = Context()
    usagelog = ctx.usagelog
    # Did we get anything?
    if len(headers) > 0:
        # Loop through targets frames we found
        for header in headers:
            # Get a cal object for this target data
            c = get_cal_object(session, None, header=header)

            yield dict(
                label    = header.data_label,
                filename = header.diskfile.file.name,
                md5      = header.diskfile.data_md5,
                cal_info = cals_info(c, caltype, qtype='GET',
                                     log=ctx.req.log,
                                     add_note=usagelog.add_note,
                                     hostname=ctx.req.env.server_hostname),
                )

    # commit the usagelog notes
    session.commit()

@templating.templated("calmgr.xml", content_type='text/xml', with_session=True)
def calmgr(session, req, selection):
    """
    This is the calibration manager. It implements a machine readable calibration association server
    req is an apache request handler object
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

    returns an apache request status code
    """

    # There selection has to be a closed query. If it's open, then disallow
    if openquery(selection):
        # writing stuff here causes apache to send a 200 OK rather than the 406
        #req.content_type = "text/plain"
        #req.write("<!-- Error: Selection cannot represent an open query for calibration association -->\n\n")
        raise templating.SkipTemplateError(HTTP_NOT_ACCEPTABLE)

    # Only the canonical versions
    selection['canonical'] = True

    # Was the request for only one type of calibration?
    caltype = selection.get('caltype', '')


    # An empty cal type is acceptable for GET - means to list all the calibrations available
    if not caltype and req.method == 'POST':
        req.content_type = "text/plain"
        req.write("<!-- Error: No calibration type specified-->\n\n")
        raise templating.SkipTemplateError(HTTP_NOT_ACCEPTABLE)

    gen = (generate_post_calmgr if req.method == 'POST' else generate_get_calmgr)

    return dict(
        machine_name = os.uname()[1],
        req_method   = req.method,
        now          = datetime.datetime.now(),
        utcnow       = datetime.datetime.utcnow(),
        generator    = gen(session, req, selection, caltype),
        )
