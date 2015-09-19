# This is the apache python handler
# See /etc/httpd/conf.d/python.conf
# When a request comes in, handler(req) gets called by the apache server

import sys
import re
from gemini_metadata_utils import gemini_date

from mod_python import apache
from mod_python import util

from fits_storage_config import blocked_urls, use_as_archive
from web.summary import summary
from web.file_list import xmlfilelist, jsonfilelist, jsonsummary, jsonqastate
from web.tapestuff import fileontape, tape, tapewrite, tapefile, taperead
from web.xml_tape import xmltape
from web.progsobserved import progsobserved
from web.gmoscal import gmoscal
from web.notification import notification, import_odb_notifications
from web.calmgr import calmgr
from web.calibrations import calibrations
from web.upload_file import upload_file
from web.curationreport import curation_report
from web.standards import standardobs
from web.selection import getselection
from web.fileserver import fileserver, download
from web.qastuff import qareport, qametrics, qaforgui
from web.statistics import content, stats
from web.user import request_account, password_reset, request_password_reset, login, logout, whoami, change_password
from web.user import staff_access, user_list, userfromcookie, AccessForbidden
from web.userprogram import my_programs
from web.searchform import searchform, nameresolver
from web.logreports import usagereport, usagedetails, downloadlog, usagestats
from web.preview import preview
from web.obslogs import obslogs
from web.reporting import report
from web.queuestatus import queuestatus
from web.api import update_headers
from web import templating

from orm import session_scope, NoResultFound
from orm.usagelog import UsageLog

from functools import partial

####### HELPER FUNCTIONS #######

# Send debugging info to browser
def debugmessage(req):
    req.content_type = "text/plain"
    req.write("Debug info\n\n")
    req.write("python interpreter name: %s\n\n" % (str(req.interpreter)))
    req.write("Pythonpath: %s\n\n" % (str(sys.path)))
    req.write("python path: \n")
    for i in sys.path:
        req.write("-- %s\n" % i)
    req.write("\n")
    req.write("uri: %s\n\n" % (str(req.uri)))
    req.write("unparsed_uri: %s\n\n" % (str(req.unparsed_uri)))
    req.write("the_request: %s\n\n" % (str(req.the_request)))
    req.write("filename: %s\n\n" % (str(req.filename)))
    req.write("hostname: %s\n\n" % (str(req.hostname)))
    req.write("canonical_filename: %s\n\n" % (str(req.canonical_filename)))
    req.write("path_info: %s\n\n" % (str(req.path_info)))
    req.write("args: %s\n\n" % (str(req.args)))

    req.write("User agent: %s\n\n" % str(req.headers_in['User-Agent']))
    req.write("All Headers in: %s\n\n" % str(req.headers_in))
    return apache.HTTP_OK
####### END HELPER FUNCTIONS #######


#### STANDARD ROUTING ####

# The following mappings connect actions with the functions that will produce the
# content. They're simple function calls, like
#
#   'debug'        -> debugmessage(req)
#   'nameresolver' -> nameresolver(req, things)
#   'calibrations' -> calibrations(req, selections)
#
# These three patterns are the most common for the archive web actions, which makes
# detecting and invoking them a task for boilerplate code. Moving everything up here,
# instead, simplifies the handler, which is left with calls that require more
# preprocessing, or invocations with more specific arguments.

# Functions invoked with (req)
mapping_simple = {
    'debug': debugmessage,                                  # A debug util
    'content': content,                                     # Database Statistics
    'stats': stats,
    'qareport': qareport,                                   # Submit QA metric measurement report
    'usagereport': usagereport,                             # Usage Statistics and Reports
    'usagestats': usagestats,                               # Usage Stats
    'xmltape': xmltape,                                     # XML Tape handler
    'taperead': taperead,                                   # TapeRead handler
    'notification': notification,                           # Emailnotification handler
    'import_odb_notifications': import_odb_notifications,   # Notification update from odb handler
    'request_password_reset': request_password_reset,       # request password reset email
    'logout': logout,                                       # logout
    'user_list': user_list,                                 # user_list
    'update_headers': update_headers,                       # JSON RPC dispatcher
    }

# Functions invoked with (req, things)
mapping_things = {
    'nameresolver': nameresolver,       # Name resolver proxy
    'fileontape': fileontape,           # The fileontape handler
    'file': fileserver,                 # This is the fits file server
    'download': download,
    'qametrics': qametrics,             # Retrieve QA metrics, simple initial version
    'qaforgui': qaforgui,               # Retrieve QA metrics, json version for GUI
    'usagedetails': usagedetails,       # Usage Reports
    'downloadlog': downloadlog,         # Download log
    'tape': tape,                       # Tape handler
    'tapewrite': tapewrite,             # TapeWrite handler
    'tapefile': tapefile,               # TapeFile handler
    'curation': curation_report,        # curation_report handler
    'request_account': request_account, # new account request
    'password_reset': password_reset,   # account password reset request
    'login': login,                     # login form
    'whoami': whoami,                   # whoami
    'change_password': change_password, # change_password
    'my_programs': my_programs,         # my_programs
    'staff_access': staff_access,       # staff_access
    'preview': preview,                 # previews
    'queuestatus': queuestatus,         # Show some info on what's going on with the queues
}

# Functions invoked with (req, selections)
mapping_selection = {
    'calibrations': calibrations,   # The calibrations handler
    'xmlfilelist': xmlfilelist,     # The xml and json file list handlers
    'jsonfilelist': jsonfilelist,
    'jsonsummary': jsonsummary,
    'jsonqastate': jsonqastate,
    'calmgr': calmgr,               # The calmgr handler
    'gmoscal': gmoscal,             # The GMOS twilight flat and bias report


    # Obslogs get their own summary-like handler
    # Both actions use the same function, with 'sumtype' specifiying which
    # one of them, but aside from that, it's just a regular (req, selections)
    # We're using functools.partial here to pin sumtype's value
    'obslogs': partial(obslogs, sumtype='obslogs'),
    'associated_obslogs': partial(obslogs, sumtype='obslogs'),

    # The GMOS twilight flat and bias report (JSON result)
    # The function here is the same as for 'gmoscal'. We're using partial for
    # the same reason as with obslogs (see above)
    'gmoscaljson': partial(gmoscal, do_json=True),
}

#### END STANDARD ROUTING ####

# The top top level handler. This simply wraps thehandler with logging funcitons
def handler(req):
    # Instantiate the UsageLog instance for this request, populate initial values from req
    # and stuff it into the request object
    req.usagelog = UsageLog(req)

    # Add the log to the database
    # We need to do that here, so there we can reference it in the querylog etc from within the handler call tree
    with session_scope() as session:
        try:
            user = userfromcookie(session, req)
            if user:
                req.usagelog.user_id = user.id
            session.add(req.usagelog)
            session.commit()

            # Call the actual handler
            retary = thehandler(req)
            req.status = retary

        # util.redirect raises this exception.
        # Log it as a redirect (303)
        except apache.SERVER_RETURN:
            req.status = 303
            raise

        except AccessForbidden as e:
            req.status = 403
            req.content_type = e.content_type
            req.write(templating.get_env().get_template(e.template).render(message = e.message))
            if e.annotate is not None:
                annotationClass = e.annotate
                try:
                    log = session.query(annotationClass).filter(annotationClass.usagelog_id == req.usagelog.id).one()
                except NoResultFound:
                    log = annotationClass(req.usagelog)
                log.add_note(e.message)
                session.add(log)
            retary = apache.OK

        except Exception:
            req.status = apache.HTTP_INTERNAL_SERVER_ERROR
            raise
        finally:
            # Grab the final log values
            req.usagelog.set_finals(req)
            session.commit()
            session.close()

        if retary in (apache.OK, apache.HTTP_OK):
            return apache.OK
        return retary

# The top level handler. This essentially calls out to the specific
# handler function depending on the uri that we're handling
def thehandler(req):
    # Set the no_cache flag on all our output
    # no_cache is not writable, have to set the headers directly
    req.headers_out['Cache-Control'] = 'no-cache'
    req.headers_out['Expired'] = '-1'

    # First check if the request went to an archive machine with an unqualified host name.
    # We re-direct to the fully qualified version so that cookie names are consistent.
    if req.hostname == 'archive':
        new_uri = "https://archive.gemini.edu%s" % req.unparsed_uri
        util.redirect(req, new_uri)

    # Parse the uri we were given.
    # This gives everything from the uri below the handler
    # eg if we're handling /python and we're the client requests
    # http://server/python/a/b.fits then we get a/b.fits

    # Use the unparsed_uri as there may be encoded slashes in it that get parsed and we dont want those parsed.
    uri = req.unparsed_uri

    # But then we need to manually split off ?arguments
    uri = uri.split('?')[0]

    # Split this about any /-es to get the "things" in the URL
    things = uri.split('/')

    # Remove any blanks- from double slashed in the URL
    while things.count(''):
        things.remove('')

    # Check if it's empty, redirect as apopriate
    if len(things) == 0:
        # Empty request
        if use_as_archive:
            util.redirect(req, "/searchform")
        else:
            util.redirect(req, "/usage.html")

    # Extract the main action
    this = things.pop(0)
    req.usagelog.this = this

    if this in blocked_urls:
        return apache.HTTP_FORBIDDEN

    # At this point we can route the easier actions
    if this in mapping_simple:
        return mapping_simple[this](req)
    if this in mapping_things:
        return mapping_things[this](req, things)
    if this in mapping_selection:
        return mapping_selection[this](req, getselection(things))

    # Before we process the request, parse any arguments into a list
    args = []
    if req.args:
        args = req.args.split('&')
        while args.count(''):
            args.remove('')
    # Parse the arguments here too
    # All we have for now are order_by arguments - form a list of order_by keywords
    orderby = []
    for i in range(len(args)):
        match = re.match(r'orderby\=(\S*)', args[i])
        if match:
            orderby.append(match.group(1))

    # OK, parse and action the rest main URL things

    # Archive searchform
    if this == 'searchform':
        return searchform(req, things, orderby)

    # This is the header summary handler
    if this in {'summary', 'diskfiles', 'ssummary', 'lsummary', 'searchresults', 'associated_cals'}:
        # the nolinks feature is used especially in external email notifications
        try:
            things.remove('nolinks')
            links = False
        except ValueError:
            links = True
        # the body_only feature is used when embedding the summary
        try:
            things.remove('body_only')
            body_only = True
        except ValueError:
            body_only = False

        # Parse the rest of the uri here while we're at it
        # Expect some combination of program_id, observation_id, date and instrument name
        # We put the ones we got in a dictionary
        selection = getselection(things)

        retval = summary(req, this, selection, orderby, links=links, body_only=body_only)
        return retval

    # This is the standard star in observation server
    if this == 'standardobs':
        header_id = things.pop(0)
        retval = standardobs(req, header_id)
        return retval

    # The processed_cal upload server
    if this == 'upload_processed_cal':
        if len(things) != 1:
            return apache.HTTP_NOT_ACCEPTABLE
        retval = upload_file(req, things[0], processed_cal=True)
        return retval

    # The generic upload_file server
    if this == 'upload_file':
        retval = upload_file(req, things[0])
        return retval

    # This returns the fitsverify, mdreport or fullheader text from the database
    # you can give it either a diskfile_id or a filename
    if this in ['fitsverify', 'mdreport', 'fullheader']:
        try:
            return report(req, thing=things.pop(0))
        except IndexError:
            req.content_type = "text/plain"
            req.write("You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
            return apache.HTTP_OK

    # This is the projects observed feature
    if this == "programsobserved":
        selection = getselection(things)
        if ("date" not in selection) and ("daterange" not in selection):
            selection["date"] = gemini_date("today")
        retval = progsobserved(req, selection)
        return retval

    # Static files are listed in the apache.conf so do not even go via the python handler now.
    #staticfiles = ["robots.txt", "favicon.ico", "jquery-1.11.1.min.js", "test.html", "test2.html"]
    #if this in staticfiles:
        #newurl = "/htmldocs/%s" % this
        #util.redirect(req, newurl)

    # Last one on the list - if we haven't return(ed) out of this function
    # by one of the methods above, then if we're the archive we 404, else
    # we should send out the usage message
    if use_as_archive:
        return apache.HTTP_NOT_FOUND
    else:
        util.redirect(req, "/usage.html")

# End of apache handler() function.
