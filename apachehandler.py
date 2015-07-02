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
from web.file_list import xmlfilelist, jsonfilelist, jsonsummary
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
from web.user import staff_access, user_list, userfromcookie
from web.userprogram import my_programs
from web.searchform import searchform, nameresolver
from web.logreports import usagereport, usagedetails, downloadlog, usagestats
from web.preview import preview
from web.obslogs import obslogs
from web.reporting import report

from orm import sessionfactory
from orm.usagelog import UsageLog


# The top top level handler. This simply wraps thehandler with logging funcitons
def handler(req):
    # Instantiate the UsageLog instance for this request, populate initial values from req
    # and stuff it into the request object
    req.usagelog = UsageLog(req)

    # Add the log to the database
    # We need to do that here, so there we can reference it in the querylog etc from within the handler call tree
    try:
        session = sessionfactory()
        user = userfromcookie(session, req)
        if user:
            req.usagelog.user_id = user.id
        session.add(req.usagelog)
        session.commit()

        # Call the actual handler
        retary = thehandler(req)

        # Grab the final log values
        req.usagelog.set_finals(req)

        session.commit()
    finally:
        session.close()

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
        new_uri = "http://archive.gemini.edu%s" % req.unparsed_uri
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
            return usagemessage(req)

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

    # OK, parse and action the main URL things

    this = things.pop(0)
    req.usagelog.this = this

    if this in blocked_urls:
        return apache.HTTP_FORBIDDEN

    # Archive searchform
    if this == 'searchform':
        return searchform(req, things, orderby)

    # Name resolver proxy
    if this == 'nameresolver':
        return nameresolver(req, things)

    # A debug util
    if this == 'debug':
        return debugmessage(req)

    # This is the header summary handler
    if this in ['summary', 'diskfiles', 'ssummary', 'lsummary', 'searchresults', 'associated_cals']:
        links = True
        # the nolinks feature is used especially in external email notifications
        if 'nolinks' in things:
            links = False
            things.remove('nolinks')

        # Parse the rest of the uri here while we're at it
        # Expect some combination of program_id, observation_id, date and instrument name
        # We put the ones we got in a dictionary
        selection = getselection(things)

        retval = summary(req, this, selection, orderby, links)
        return retval

    # Obslogs get their own summary-like handler
    if this in ['obslogs', 'associated_obslogs']:
        # Only some of the selection is relevant, but that's fine, we can still parse it.
        selection = getselection(things)

        retval = obslogs(req, this, selection)

        return retval



    # This is the standard star in observation server
    if this == 'standardobs':
        header_id = things.pop(0)
        retval = standardobs(req, header_id)
        return retval


    # The calibrations handler
    if this == 'calibrations':
        # Parse the rest of the URL.
        selection = getselection(things)

        # If we want other arguments like order by
        # we should parse them here

        retval = calibrations(req, selection)
        return retval

    # The xml and json file list handlers
    if this == 'xmlfilelist':
        selection = getselection(things)
        retval = xmlfilelist(req, selection)
        return retval
    if this == 'jsonfilelist':
        selection = getselection(things)
        retval = jsonfilelist(req, selection)
        return retval
    if this == 'jsonsummary':
        selection = getselection(things)
        retval = jsonsummary(req, selection)
        return retval

    # The fileontape handler
    if this == 'fileontape':
        retval = fileontape(req, things)
        return retval

    # The calmgr handler
    if this == 'calmgr':
        # Parse the rest of the URL.
        selection = getselection(things)

        # If we want other arguments like order by
        # we should parse them here

        retval = calmgr(req, selection)
        return retval

    # The processed_cal upload server
    if this == 'upload_processed_cal':
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
            return apache.OK

    # This is the fits file server
    if this == 'file':
        return fileserver(req, things)

    # This is the fits file server
    if this == 'download':
        return download(req, things)

    # This is the projects observed feature
    if this == "programsobserved":
        selection = getselection(things)
        if ("date" not in selection) and ("daterange" not in selection):
            selection["date"] = gemini_date("today")
        retval = progsobserved(req, selection)
        return retval

    # The GMOS twilight flat and bias report
    if this == "gmoscal":
        selection = getselection(things)
        retval = gmoscal(req, selection)
        return retval

    # The GMOS twilight flat and bias report
    if this == "gmoscaljson":
        selection = getselection(things)
        retval = gmoscal(req, selection, do_json=True)
        return retval

    # Submit QA metric measurement report
    if this == "qareport":
        return qareport(req)

    # Retrieve QA metrics, simple initial version
    if this == "qametrics":
        return qametrics(req, things)

    # Retrieve QA metrics, json version for GUI
    if this == "qaforgui":
        return qaforgui(req, things)

    # Database Statistics
    if this == "content":
        return content(req)

    if this == "stats":
        return stats(req)

    # Usage Statistics and Reports
    if this == "usagereport":
        return usagereport(req)

    # Usage Reports
    if this == "usagedetails":
        return usagedetails(req, things)

    # Download log 
    if this == "downloadlog":
        return downloadlog(req, things)

    # Usage Stats 
    if this == "usagestats":
        return usagestats(req)

    # Tape handler
    if this == "tape":
        return tape(req, things)

    # TapeWrite handler
    if this == "tapewrite":
        return tapewrite(req, things)

    # TapeFile handler
    if this == "tapefile":
        return tapefile(req, things)

    # TapeRead handler
    if this == "taperead":
        return taperead(req, things)

    # XML Tape handler
    if this == "xmltape":
        return xmltape(req)

    # Emailnotification handler
    if this == "notification":
        return notification(req)

    # Notification update from odb handler
    if this == "import_odb_notifications":
        return import_odb_notifications(req)

    # curation_report handler
    if this == "curation":
        return curation_report(req, things)

    # new account request
    if this == "request_account":
        return request_account(req, things)

    # account password reset request
    if this == "password_reset":
        return password_reset(req, things)

    # request password reset email
    if this == "request_password_reset":
        return request_password_reset(req)

    # login form
    if this == "login":
        return login(req, things)

    # logout
    if this == "logout":
        return logout(req)

    # whoami
    if this == "whoami":
        return whoami(req, things)

    # change_password
    if this == "change_password":
        return change_password(req, things)

    # my_programs
    if this == "my_programs":
        return my_programs(req, things)

    # staff_access
    if this == "staff_access":
        return staff_access(req, things)

    # user_list
    if this == "user_list":
        return user_list(req)

    # previews
    if this == "preview":
        return preview(req, things)

    # Some static files that the server should serve via a redirect.
    staticfiles = ["robots.txt", "favicon.ico", "jquery-1.11.1.min.js", "test.html", "test2.html"]
    if this in staticfiles:
        newurl = "/htmldocs/%s" % this
        util.redirect(req, newurl)

    # Last one on the list - if we haven't return(ed) out of this function
    # by one of the methods above, then we should send out the usage message
    return usagemessage(req)

# End of apache handler() function.
# Below are various helper functions called from above.
# The web summary has its own package

# Send usage message to browser
def usagemessage(req):
    fp = open("/opt/FitsStorage/htmldocroot/htmldocs/usage.html", "r")
    stuff = fp.read()
    fp.close()
    req.content_type = "text/html"
    req.write(stuff)
    return apache.OK

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
    return apache.OK
