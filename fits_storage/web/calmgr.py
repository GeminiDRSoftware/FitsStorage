"""
This module contains the calmgr html generator function.
"""
from ..orm import sessionfactory
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..web.selection import queryselection, openquery
from ..cal import get_cal_object
from ..fits_storage_config import using_apache, storage_root, fits_servername
from ..gemini_metadata_utils import cal_types
from ..apache_return_codes import HTTP_OK, HTTP_NOT_ACCEPTABLE

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

            for cal in cals:
                # OK, say what we found
                resp.append("<calibration>")
                resp.append("<caltype>%s</caltype>" % ct)
                resp.append("<datalabel>%s</datalabel>" % cal.data_label)
                resp.append("<filename>%s</filename>" % cal.diskfile.file.name)
                resp.append("<md5>%s</md5>" % cal.diskfile.data_md5)
                if cal.diskfile.present:
                    if http:
                        resp.append("<url>http://%s/file/%s</url>" % (hostname, cal.diskfile.file.name))
                    else:
                        path = os.path.join(storage_root, cal.diskfile.file.path, cal.diskfile.file.name)
                        resp.append("<url>file://%s</url>" % path)
                else:
                    # Once we are sending new stlye processed calibrations to the GSA,
                    # we can form a URL to the GSA here and return that.
                    resp.append("<!-- Calibration Result found in DB, but file is not present on FITS server -->")
                resp.append("</calibration>")
                add_note("CalMGR %s returning %s %s" % (qtype, ct, cal.diskfile.file.name))
            if len(cals) == 0:
                resp.append("<!-- NO CALIBRATION FOUND for caltype %s-->" % ct)
                add_note("CalMGR %s - no calibration type %s found" % (qtype, ct))
        except Exception:
            resp.append("<!-- PROBLEM WHILE SEARCHING FOR caltype %s-->" % ct)
            string = traceback.format_tb(sys.exc_info()[2])
            string = "".join(string)
            log("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
            add_note("Exception in cal association: %s: %s %s" % (sys.exc_info()[0], sys.exc_info()[1], string))

    return '\n'.join(resp) + '\n'

def calmgr(req, selection):
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
        req.content_type = "text/plain"
        req.write("<!-- Error: Selection cannot represent an open query for calibration association -->\n")
        return HTTP_NOT_ACCEPTABLE

    # Only the canonical versions
    selection['canonical'] = True

    session = sessionfactory()
    try:
        # Was the request for only one type of calibration?
        caltype = ''
        if 'caltype' in selection:
            caltype = selection['caltype']
        # An empty cal type is acceptable for GET - means to list all the calibrations available
        elif req.method == 'POST':
            req.content_type = "text/plain"
            req.write("<!-- Error: No calibration type specified-->\n")
            return HTTP_NOT_ACCEPTABLE

        # Did we get called via an HTTP POST or HTTP GET?
        if req.method == 'POST':
            # OK, get the details from the POST data
            req.content_type = "text/plain"
            clientdata = req.read()
            #req.write("\nclient data: %s\n" % clientdata)
            clientstr = urllib.unquote_plus(clientdata)
            #req.write("\nclient str: %s\n" % clientstr)
            match = re.match("descriptors=(.*)&types=(.*)", clientstr)
            desc_str = match.group(1)
            type_str = match.group(2)
            #req.write("\ndesc_str: %s\n" % desc_str)
            #req.write("\ntype_str: %s\n" % type_str)
            descriptors = eval(desc_str)
            types = eval(type_str)
            req.usagelog.add_note("CalMGR request Descriptor Dictionary: %s" % descriptors)
            req.usagelog.add_note("CalMGR request Types List: %s" % types)

            # OK, there are a couple of items that are handled in the DB as if they are descriptors
            # but they're actually types. This is where we push them into the descriptor disctionary
            descriptors['nodandshuffle'] = 'GMOS_NODANDSHUFFLE' in types
            descriptors['spectroscopy'] = 'SPECT' in types
            descriptors['overscan_subtracted'] = 'OVERSCAN_SUBTRACTED' in types
            descriptors['overscan_trimmed'] = 'OVERSCAN_TRIMMED' in types
            descriptors['prepared'] = 'PREPARED' in types


            # Get a cal object for this target data
            req.content_type = "text/xml"
            req.write('<?xml version="1.0" encoding="UTF-8"?>')
            req.write('<calibration_associations xmlns="http://www.gemini.edu/xml/gsaCalibrations/v1.0">\n')
            req.write('<!-- Generated by %s for POST request at %s / %s UTC-->' % (os.uname()[1],
                            datetime.datetime.now(), datetime.datetime.utcnow()))
            req.write("<dataset>\n")
            req.write("<datalabel>%s</datalabel>\n" % descriptors['data_label'])

            c = get_cal_object(session, None, header=None, descriptors=descriptors, types=types)

            req.write(cals_info(c, caltype, qtype='GET',
                                log=req.log_error,
                                add_note=req.usagelog.add_note,
                                hostname=fits_servername,
                                storage_root=storage_root))

            req.write("</dataset>\n")
            req.write("</calibration_associations>\n")

            # Commit the changes to the usagelog
            session.commit()
            return HTTP_OK

        else:
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
            if openquery(selection):
                query = query.limit(1000)

            # OK, do the query
            headers = query.all()

            req.content_type = "text/xml"
            req.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            req.write('<calibration_associations xmlns="http://www.gemini.edu/xml/gsaCalibrations/v1.0">\n')
            req.write('<!-- Generated by %s for GET request at %s / %s UTC-->' % (os.uname()[1],
                            datetime.datetime.now(), datetime.datetime.utcnow()))
            # Did we get anything?
            if len(headers) > 0:
                # Loop through targets frames we found
                for header in headers:
                    req.write("<dataset>\n")
                    req.write("<datalabel>%s</datalabel>\n" % header.data_label)
                    req.write("<filename>%s</filename>\n" % header.diskfile.file.name)
                    req.write("<md5>%s</md5>\n" % header.diskfile.data_md5)

                    # Get a cal object for this target data
                    c = get_cal_object(session, None, header=header)

                    req.write(cals_info(c, caltype, qtype='GET',
                                        log=req.log_error,
                                        add_note=req.usagelog.add_note,
                                        hostname=req.server.server_hostname))

                    req.write("</dataset>\n")
            else:
                req.write("<!-- COULD NOT LOCATE METADATA FOR DATASET -->\n")

            req.write("</calibration_associations>\n")
            # commit the usagelog notes
            session.commit()
            return HTTP_OK
    except IOError:
        pass
    finally:
        session.close()

