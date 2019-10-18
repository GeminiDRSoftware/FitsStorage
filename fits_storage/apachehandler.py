#O # This is the apache python handler
#O # See /etc/httpd/conf.d/python.conf
#O # When a request comes in, handler(req) gets called by the apache server
#O 
#O import sys
#O import re
#O 
#O #O from .utils.web import get_context, context_wrapped, ModPythonRequest, ModPythonResponse
#O from .utils.web import get_context, context_wrapped
#O import traceback
#O from .gemini_metadata_utils import gemini_date
#O 
#O from mod_python import apache
#O from mod_python import util
#O 
#O from .fits_storage_config import blocked_urls, use_as_archive
#O from .web.summary import summary
#O from .web.file_list import xmlfilelist, jsonfilelist, jsonsummary, jsonqastate
#O from .web.tapestuff import fileontape, tape, tapewrite, tapefile, taperead
#O from .web.xml_tape import xmltape
#O from .web.progsobserved import progsobserved, sitemap
#O from .web.gmoscal import gmoscal_html, gmoscal_json
#O from .web.notification import notification, import_odb_notifications
#O from .web.calmgr import calmgr
#O from .web.calibrations import calibrations
#O from .web.upload_file import upload_file
#O from .web.curationreport import curation_report
#O from .web.standards import standardobs
#O from .web.selection import getselection
#O from .web.fileserver import fileserver, download
#O from .web.qastuff import qareport, qametrics, qaforgui
#O from .web.statistics import content, stats
#O from .web.user import request_account, password_reset, request_password_reset, login, logout, whoami, change_password
#O from .web.user import staff_access, user_list, AccessForbidden
#O from .web.userprogram import my_programs
#O from .web.searchform import searchform, nameresolver
#O from .web.logreports import usagereport, usagedetails, downloadlog, usagestats
#O from .web.preview import preview
#O from .web.obslogs import obslogs
#O from .web.reporting import report
#O from .web.queuestatus import queuestatus
#O from .web.api import update_headers, ingest_files
#O from .web.miscfiles import miscfiles
#O from .web import templating
#O 
#O from .orm import session_scope, NoResultFound
#O from .orm.usagelog import UsageLog
#O 
#O from functools import partial
#O 
#O ####### HELPER FUNCTIONS #######
#O 
#O # Send debugging info to browser
#O def debugmessage(req):
#O     req.content_type = "text/plain"
#O     req.write("Debug info\n\n")
#O     req.write("python interpreter name: %s\n\n" % (str(req.interpreter)))
#O     req.write("Pythonpath: %s\n\n" % (str(sys.path)))
#O     req.write("python path: \n")
#O     for i in sys.path:
#O         req.write("-- %s\n" % i)
#O     req.write("\n")
#O     req.write("uri: %s\n\n" % (str(req.uri)))
#O     req.write("unparsed_uri: %s\n\n" % (str(req.unparsed_uri)))
#O     req.write("the_request: %s\n\n" % (str(req.the_request)))
#O     req.write("filename: %s\n\n" % (str(req.filename)))
#O     req.write("hostname: %s\n\n" % (str(req.hostname)))
#O     req.write("canonical_filename: %s\n\n" % (str(req.canonical_filename)))
#O     req.write("path_info: %s\n\n" % (str(req.path_info)))
#O     req.write("args: %s\n\n" % (str(req.args)))
#O 
#O     req.write("User agent: %s\n\n" % str(req.headers_in['User-Agent']))
#O     req.write("All Headers in: %s\n\n" % str(req.headers_in))
#O     return apache.HTTP_OK
#O ####### END HELPER FUNCTIONS #######
#O 
#O 
#O #### STANDARD ROUTING ####
#O 
#O # The following mappings connect actions with the functions that will produce the
#O # content. They're simple function calls, like
#O #
#O #   'debug'        -> debugmessage()
#O #   'nameresolver' -> nameresolver(things)
#O #   'calibrations' -> calibrations(selections)
#O #
#O # These three patterns are the most common for the archive web actions, which makes
#O # detecting and invoking them a task for boilerplate code. Moving everything up here,
#O # instead, simplifies the handler, which is left with calls that require more
#O # preprocessing, or invocations with more specific arguments.
#O 
#O # Functions invoked with (req)
#O mapping_simple = {
#O     'debug': debugmessage,                                  # A debug util
#O     'content': content,                                     # Database Statistics
#O     'stats': stats,
#O     'qareport': qareport,                                   # Submit QA metric measurement report
#O     'usagereport': usagereport,                             # Usage Statistics and Reports
#O     'usagestats': usagestats,                               # Usage Stats
#O     'xmltape': xmltape,                                     # XML Tape handler
#O     'taperead': taperead,                                   # TapeRead handler
#O     'notification': notification,                           # Emailnotification handler
#O     'import_odb_notifications': import_odb_notifications,   # Notification update from odb handler
#O     'request_password_reset': request_password_reset,       # request password reset email
#O     'logout': logout,                                       # logout
#O     'user_list': user_list,                                 # user_list
#O     'update_headers': update_headers,                       # JSON RPC dispatcher
#O     'ingest_files': ingest_files,                           # JSON RPC dispatcher
#O     'curation': curation_report,                            # curation_report handler
#O     'sitemap.xml': sitemap,                                 # sitemap.xml for google et al
#O     }
#O 
#O # Functions invoked with (req, things)
#O mapping_things = {
#O     'nameresolver': nameresolver,       # Name resolver proxy
#O     'fileontape': fileontape,           # The fileontape handler
#O     'file': fileserver,                 # This is the fits file server
#O     'download': download,
#O     'qametrics': qametrics,             # Retrieve QA metrics, simple initial version
#O     'qaforgui': qaforgui,               # Retrieve QA metrics, json version for GUI
#O     'usagedetails': usagedetails,       # Usage Reports
#O     'downloadlog': downloadlog,         # Download log
#O     'tape': tape,                       # Tape handler
#O     'tapewrite': tapewrite,             # TapeWrite handler
#O     'tapefile': tapefile,               # TapeFile handler
#O     'request_account': request_account, # new account request
#O     'password_reset': password_reset,   # account password reset request
#O     'login': login,                     # login form
#O     'whoami': whoami,                   # whoami
#O     'change_password': change_password, # change_password
#O     'my_programs': my_programs,         # my_programs
#O     'staff_access': staff_access,       # staff_access
#O     'preview': preview,                 # previews
#O     'queuestatus': queuestatus,         # Show some info on what's going on with the queues
#O     'miscfiles': miscfiles,             # Miscellanea (Opaque files)
#O }
#O 
#O # Functions invoked with (req, selections)
#O mapping_selection = {
#O     'calibrations': calibrations,   # The calibrations handler
#O     'xmlfilelist': xmlfilelist,     # The xml and json file list handlers
#O     'jsonfilelist': jsonfilelist,
#O     'jsonfilenames': partial(jsonfilelist, fields={'name'}),
#O     'jsonsummary': jsonsummary,
#O     'jsonqastate': jsonqastate,
#O     'calmgr': calmgr,               # The calmgr handler
#O     'gmoscal': gmoscal_html,        # The GMOS twilight flat and bias report
#O 
#O 
#O     # Obslogs get their own summary-like handler
#O     # Both actions use the same function, with 'sumtype' specifiying which
#O     # one of them, but aside from that, it's just a regular (req, selections)
#O     # We're using functools.partial here to pin sumtype's value
#O     'obslogs': partial(obslogs, sumtype='obslogs'),
#O     'associated_obslogs': partial(obslogs, sumtype='associated_obslogs'),
#O 
#O     # The GMOS twilight flat and bias report (JSON result)
#O     # The function here is the same as for 'gmoscal'. We're using partial for
#O     # the same reason as with obslogs (see above)
#O     'gmoscaljson': gmoscal_json,
#O }
#O 
#O #### END STANDARD ROUTING ####
#O 
#O # The top top level handler. This simply wraps thehandler with logging funcitons
#O @context_wrapped
#O def handler(ctx, req):
#O     with session_scope() as session:
#O         # Instantiate the UsageLog instance for this request, populate initial values from req
#O         request = ModPythonRequest(session, req)
#O         response = ModPythonResponse(session, req)
#O         ctx.setContent(request, response)
#O 
#O         usagelog = UsageLog(ctx)
#O         ctx.usagelog = usagelog
#O         ctx.session = session
#O 
#O         try:
#O             try:
#O                 usagelog.user_id = request.user.id
#O             except AttributeError:
#O                 # No user defined
#O                 pass
#O             session.add(usagelog)
#O             session.commit()
#O 
#O             # Call the actual handler
#O             thehandler(req)
#O             retary = req.status = ctx.resp.status
#O 
#O         # util.redirect raises this exception.
#O         # Log it as a redirect (303)
#O         except apache.SERVER_RETURN:
#O             req.status = 303
#O             raise
#O 
#O         except AccessForbidden as e:
#O             req.status = 403
#O             req.content_type = e.content_type
#O             req.write(templating.get_env().get_template(e.template).render(message = e.message))
#O             if e.annotate is not None:
#O                 annotationClass = e.annotate
#O                 try:
#O                     log = session.query(annotationClass).filter(annotationClass.usagelog_id == usagelog.id).one()
#O                 except NoResultFound:
#O                     log = annotationClass(usagelog)
#O                 log.add_note(e.message)
#O                 session.add(log)
#O             retary = apache.OK
#O 
#O         except templating.TemplateAccessError as e:
#O             retary = req.status = apache.HTTP_SERVICE_UNAVAILABLE
#O             usagelog.add_note("Can't access template '{}'".format(str(e)))
#O 
#O         except (IOError, templating.InterruptedError):
#O             # HTTP 499 is a non-standard code used by a number of web servers. Nginx defines it
#O             # as Client Closed Request. We'll stick to that meaning.
#O             req.status = 499
#O             retary = apache.OK
#O 
#O         except Exception as e:
#O             req.status = apache.HTTP_INTERNAL_SERVER_ERROR
#O             req.usagelog.add_note("Exception '{}'".format(str(e)))
#O             string = traceback.format_tb(sys.exc_info()[2])
#O             string = "".join(string)
#O             req.usagelog.add_note("Exception: %s : %s... %s" % (sys.exc_info()[0], sys.exc_info()[1], string))
#O             raise
#O         finally:
#O             # Grab the final log values
#O             # Make sure that the session is in a consistent state
#O             session.commit()
#O             usagelog.set_finals(ctx)
#O             session.commit()
#O             session.close()
#O 
#O         if retary in (apache.OK, apache.HTTP_OK):
#O             return apache.OK
#O         return retary
#O 
#O # The top level handler. This essentially calls out to the specific
#O # handler function depending on the uri that we're handling
#O def thehandler(req):
#O     # Set the no_cache flag on all our output
#O     # no_cache is not writable, have to set the headers directly
#O     req.headers_out['Cache-Control'] = 'no-cache'
#O     req.headers_out['Expired'] = '-1'
#O 
#O     # First check if the request went to an archive machine with an unqualified host name.
#O     # We re-direct to the fully qualified version so that cookie names are consistent.
#O     if req.hostname == 'archive':
#O         new_uri = "https://archive.gemini.edu%s" % req.unparsed_uri
#O         util.redirect(req, new_uri)
#O 
#O     # Parse the uri we were given.
#O     # This gives everything from the uri below the handler
#O     # eg if we're handling /python and we're the client requests
#O     # http://server/python/a/b.fits then we get a/b.fits
#O 
#O     # Use the unparsed_uri as there may be encoded slashes in it that get parsed and we dont want those parsed.
#O     uri = req.unparsed_uri
#O 
#O     # But then we need to manually split off ?arguments
#O     uri = uri.split('?')[0]
#O 
#O     # Split this about any /-es to get the "things" in the URL
#O     things = uri.split('/')
#O 
#O     # Remove any blanks- from double slashed in the URL
#O     while things.count(''):
#O         things.remove('')
#O 
#O     # Check if it's empty, redirect as apopriate
#O     if len(things) == 0:
#O         # Empty request
#O         if use_as_archive:
#O             util.redirect(req, "/searchform")
#O         else:
#O             util.redirect(req, "/usage.html")
#O 
#O     # Extract the main action
#O     this = things.pop(0)
#O     get_context().usagelog.this = this
#O 
#O     if this in blocked_urls:
#O         return apache.HTTP_FORBIDDEN
#O 
#O     # At this point we can route the easier actions
#O     if this in mapping_simple:
#O         return mapping_simple[this]()
#O     if this in mapping_things:
#O         return mapping_things[this](things)
#O     if this in mapping_selection:
#O         return mapping_selection[this](getselection(things))
#O 
#O     # Before we process the request, parse any arguments into a list
#O     args = []
#O     if req.args:
#O         args = req.args.split('&')
#O         while args.count(''):
#O             args.remove('')
#O     # Parse the arguments here too
#O     # All we have for now are order_by arguments - form a list of order_by keywords
#O     orderby = []
#O     for i in range(len(args)):
#O         match = re.match(r'orderby\=(\S*)', args[i])
#O         if match:
#O             orderby.append(match.group(1))
#O 
#O     # OK, parse and action the rest main URL things
#O 
#O     # Archive searchform
#O     if this == 'searchform':
#O         return searchform(things, orderby)
#O 
#O     # This is the header summary handler
#O     if this in {'summary', 'diskfiles', 'ssummary', 'lsummary', 'searchresults', 'associated_cals'}:
#O         # the nolinks feature is used especially in external email notifications
#O         try:
#O             things.remove('nolinks')
#O             links = False
#O         except ValueError:
#O             links = True
#O         # the body_only feature is used when embedding the summary
#O         try:
#O             things.remove('body_only')
#O             body_only = True
#O         except ValueError:
#O             body_only = False
#O 
#O         # Parse the rest of the uri here while we're at it
#O         # Expect some combination of program_id, observation_id, date and instrument name
#O         # We put the ones we got in a dictionary
#O         selection = getselection(things)
#O 
#O         retval = summary(this, selection, orderby, links=links, body_only=body_only)
#O         return retval
#O 
#O     # This is the standard star in observation server
#O     if this == 'standardobs':
#O         header_id = things.pop(0)
#O         retval = standardobs(header_id)
#O         return retval
#O 
#O     # The processed_cal upload server
#O     if this == 'upload_processed_cal':
#O         if len(things) != 1:
#O             return apache.HTTP_NOT_ACCEPTABLE
#O         retval = upload_file(things[0], processed_cal=True)
#O         return retval
#O 
#O     # The generic upload_file server
#O     if this == 'upload_file':
#O         retval = upload_file(things[0])
#O         return retval
#O 
#O     # This returns the fitsverify, mdreport or fullheader text from the database
#O     # you can give it either a diskfile_id or a filename
#O     if this in ['fitsverify', 'mdreport', 'fullheader']:
#O         try:
#O             return report(thing=things.pop(0))
#O         except IndexError:
#O             req.content_type = "text/plain"
#O             req.write("You must specify a filename or diskfile_id, eg: /fitsverify/N20091020S1234.fits\n")
#O             return apache.HTTP_OK
#O 
#O     # This is the projects observed feature
#O     if this == "programsobserved":
#O         selection = getselection(things)
#O         retval = progsobserved(selection)
#O         return retval
#O 
#O     # Static files are listed in the apache.conf so do not even go via the python handler now.
#O     #staticfiles = ["robots.txt", "favicon.ico", "jquery-1.11.1.min.js", "test.html", "test2.html"]
#O     #if this in staticfiles:
#O         #newurl = "/htmldocs/%s" % this
#O         #util.redirect(req, newurl)
#O 
#O     # Last one on the list - if we haven't return(ed) out of this function
#O     # by one of the methods above, then if we're the archive we 404, else
#O     # we should send out the usage message
#O     if use_as_archive:
#O         return apache.HTTP_NOT_FOUND
#O     else:
#O         util.redirect(req, "/usage.html")
#O 
#O # End of apache handler() function.
