# This is the WSGI handler
# When a request comes in, handler(req) gets called by the apache server

import os
import sys
import mimetypes

from pprint import pformat

from fits_storage.utils.web import get_context, Return
from fits_storage.utils.web import ArchiveContextMiddleware
from fits_storage.utils.web import RequestRedirect, ClientError
from fits_storage.utils.web.routing import Map, Rule, BaseConverter

from fits_storage.fits_storage_config import blocked_urls, use_as_archive
from fits_storage.web.console import console, console_ingest_queue, console_export_queue, console_ingest_errors
from fits_storage.web.gmoscalfilelist import gmoscalbiasfiles
from fits_storage.web.gmoscaltwilightdetails import gmoscaltwilightdetails, gmoscaltwilightfiles
from fits_storage.web.programs import programs
from fits_storage.web.summary import summary
from fits_storage.web.file_list import xmlfilelist, jsonfilelist, jsonsummary, jsonqastate
from fits_storage.web.tapestuff import fileontape, tape, tapewrite, tapefile, taperead, jsontapefilelist
from fits_storage.web.xml_tape import xmltape
from fits_storage.web.progsobserved import progsobserved
from fits_storage.web.gmoscal import gmoscal_html, gmoscal_json
from fits_storage.web.notification import notification, import_odb_notifications
from fits_storage.web.calmgr import xmlcalmgr, jsoncalmgr
from fits_storage.web.calibrations import calibrations
from fits_storage.web.rawfiles import rawfiles
from fits_storage.web.upload_file import upload_file
from fits_storage.web.curationreport import curation_report
from fits_storage.web.standards import standardobs
from fits_storage.web.selection import getselection
from fits_storage.web.fileserver import fileserver, download, download_post
from fits_storage.web.qastuff import qareport, qametrics, qaforgui
from fits_storage.web.statistics import content, stats
from fits_storage.web.user import request_account, password_reset, request_password_reset, login, logout, whoami, \
    change_password, change_email, admin_change_email, orcid, admin_change_password, admin_file_permissions
from fits_storage.web.user import staff_access, user_list
from fits_storage.web.userprogram import my_programs
from fits_storage.web.searchform import searchform, nameresolver
from fits_storage.web.logreports import usagereport, usagedetails, downloadlog, usagestats
from fits_storage.web.preview import preview, num_previews
from fits_storage.web.obslogs import obslogs
from fits_storage.web.reporting import report
from fits_storage.web.queuestatus import queuestatus_summary, queuestatus_tb, queuestatus_update
from fits_storage.web.api import update_headers, ingest_files, ingest_programs, ingest_publications
from fits_storage.web.publication import publication_ads, list_publications
from fits_storage.web.program import program_info, program_info_json
from fits_storage.web.logcomments import log_comments
from fits_storage.web import miscfiles, miscfilesplus

from sqlalchemy.orm.exc import NoResultFound

from functools import partial

# *****************
# CUSTOM CONVERTERS


class SelectionConverter(BaseConverter):
    """
    The regular expression for this converter is very simple: just take whatever
    is left from the URL query.

    When a variable is declared along with this converter type, no other variables
    should follow it, because there will be no URL left for them.

    ``SelectionConverter`` accepts some arguments (which should be seen as "selectors"):

    * ``SEL``
    * ``ASSOC``
    * ``NOLNK``
    * ``BONLY``

    Passing no argument is equivalent to passing just ``SEL``. If any other argument
    is passed, then ``SEL`` **must** be included among the arguments. See the
    documentation for ``to_python`` method to learn about their uses.
    """

    regex = '.*'

    def __init__(self, *args):
        if args:
            if 'SEL' not in args:
                raise ValueError("One of the arguments for selection must be 'SEL'")

            # Possible values: SEL, ASSOC, NOLNK, BONLY
            self.res_order = args
        else:
            self.res_order = ['SEL']

    def to_python(self, value):
        """
        Takes the URL fed as input, breaks it into components, and then performs a nomber
        of operations over it. Finally, return a tuple.

        The number of results in the tuple will match the number of *arguments* passed to
        the converter.

        ``SEL``
          This argument is always present. It will get us the result of
          ``getselection(components)``. This is the last action to be performed, as the
          other arguments affect the ``components`` input.
        ``ASSOC``
          Will add a boolean element to the return tuple. If ``associated_calibrations``
          is found in the URL, it will be removed before the return value will be ``True``.
          Otherwise, return ``False``.
        ``NOLNK``
          Also boolean. If ``nolinks`` is found, it will be removed and the return value
          will be ``False``. Otherwise, return ``True``.
        ``BONLY``
          ``True`` if ``body_only`` is present in the URL. ``False`` otherwise.

        The order in which the arguments have been passed to the selector matters to the
        order in which their results are returned in the tuple. So, if we got
        ``<selection:foo>``, then the result is ``(getselection_dictionary,)``; for
        ``<selection(SEL,NOLNK,BONLY):sel,links,body_only)``, we'll get something
        like ``(dict, bool, bool)``; but for ``<selection(ASSOC,SEL):assoc_cals,sel>``
        we'd get ``(bool, dict)``.

        Notice that for all the examples, we've provided as many variable names as
        arguments, to make sure that the mapping between tuple arguments and variables
        is even. This is also why the order of argument matters: it makes easier to
        match the returned values to variable names.
        """
        assoc = False
        links = True
        bonly = False
        things = [v for v in value.split('/') if v != '']
        if 'ASSOC' in self.res_order:
            try:
                things.remove('associated_calibrations')
                assoc = True
            except ValueError:
                pass

        if 'NOLNK' in self.res_order:
            try:
                things.remove('nolinks')
                links = False
            except ValueError:
                pass

        if 'BONLY' in self.res_order:
            try:
                things.remove('body_only')
                bonly = True
            except ValueError:
                pass

        result = []
        for r in self.res_order:
            if r == 'SEL':
                result.append(getselection(things))
            elif r == 'ASSOC':
                result.append(assoc)
            elif r == 'NOLNK':
                result.append(links)
            elif r == 'BONLY':
                result.append(bonly)
        if len(result) == 1:
            return result[0]
        return tuple(result)


class SequenceConverter(BaseConverter):
    """
    The regular expression for this converter is very simple: just take whatever
    is left from the URL query.

    When a variable is declared along with this converter type, no other variables
    should follow it, because there will be no URL left for them.

    ``SequenceConverter`` accepts arguments. If there are any arguments, they are
    interpreted as a set of unique **allowed** values.
    """

    regex = '.*'

    def __init__(self, *args):
        self.allowed = set(args or ())

    def to_python(self, value):
        """
        Returns a list of strings, where each element is one of the components of
        the URL. Ie. if passed ``"/foo/bar/baz/"``, this function will return
        ``['foo', 'bar', 'baz']``.

        If the converter got arguments, and any of the components is not part
        of the set of arguments, this function will raise a ``ValueError``
        exception.
        """
        things = [v for v in value.split('/') if v != '']
        if self.allowed and any(th not in self.allowed for th in things):
            raise ValueError('Illegal values in the URL')

        return things


# END CUSTOM CONVERTERS
# *********************


# ****************
# HELPER FUNCTIONS


debug_template = """
Debug info

Python path:
{path}

uri: {uri}

Environment:
{env}
"""


# Send debugging info to browser
def debugmessage():
    ctx = get_context()
    req, resp = ctx.req, ctx.resp

    resp.set_content_type('text/plain')

    resp.append(debug_template.format(
        path        = '\n'.join('-- {}'.format(x) for x in sys.path),
        uri         = req.env.uri,
        env         = pformat(req.env._env)
    ))

####### END HELPER FUNCTIONS #######

url_map = Map([
    # Queries to the root should redirect to a sensible page
    Rule('/', redirect_to=('/searchform' if use_as_archive else '/usage.html')),
    Rule('/debug', debugmessage),
    Rule('/content', content),                                      # Database Statistics
    Rule('/stats', stats),
    Rule('/qareport', qareport, methods=['POST']),                  # Submit QA metric measurement report
    Rule('/usagereport', usagereport),                              # Usage Statistics and Reports
    Rule('/usagestats', usagestats),                                # Usage Stats
    Rule('/xmltape', xmltape),                                      # XML Tape handler
    Rule('/taperead', taperead),                                    # TapeRead handler
    Rule('/notification', notification),                            # Emailnotification handler
    Rule('/import_odb_notifications', import_odb_notifications,     # Notification update from odb handler
         methods=['POST']),
    Rule('/request_password_reset', request_password_reset),        # request password reset email
    Rule('/logout', logout),                                        # logout
    Rule('/user_list', user_list),                                  # user_list
    Rule('/update_headers', update_headers, methods=['POST']),      # JSON RPC dispatcher
    Rule('/ingest_files', ingest_files, methods=['POST']),          # JSON RPC dispatcher
    Rule('/ingest_programs', ingest_programs, methods=['POST']),    # JSON RPC dispatcher
    Rule('/ingest_publications', ingest_publications,
         methods=['POST']),                                          # JSON RPC dispatcher
    Rule('/publication/ads/<bibcode>', publication_ads),            # Publication ADS Info
    Rule('/list_publications', list_publications),                  # Publication Bibcode/Link List

    Rule('/curation', curation_report),                             # curation_report handler
    Rule('/staff_access', staff_access),                            # staff_access
    Rule('/admin_change_email', admin_change_email),                # admin page for changing a user's email
    Rule('/admin_change_password', admin_change_password),          # admin page for changing a user's password
    Rule('/admin_file_permissions', admin_file_permissions),        # admin page for changing custom per-file/obsid
                                                                    #   permissions

    Rule('/nameresolver/<resolver>/<target>', nameresolver),        # Name resolver proxy
    Rule('/fileontape/<filename>', fileontape),                     # The fileontape handler
    Rule('/file/<filenamegiven>', fileserver),                      # This is the fits file server
    Rule('/download/', download_post, methods=['POST']),
    Rule('/download/<selection(SEL,ASSOC):selection,associated_calibrations>',
         download, methods=['GET']),
    Rule('/qametrics/<seq_of(iq,zq,sb,pe):metrics>', qametrics),    # Retrieve QA metrics, simple initial version
    Rule('/qaforgui/<date>', qaforgui),                             # Retrieve QA metrics, json version for GUI
    Rule('/usagedetails/<int:ulid>', usagedetails),                 # Usage Reports
    Rule('/downloadlog/<seq_of:patterns>', downloadlog),            # Download log
    Rule('/tape', tape),                                            # Tape handler
    Rule('/tape/<search>', tape),                                   # Tape handler
    Rule('/tapewrite', tapewrite),                                  # TapeWrite handler
    Rule('/tapewrite/<label>', tapewrite),                          # TapeWrite handler
    Rule('/tapefile/<int:tapewrite_id>', tapefile),                 # TapeFile handler
    Rule('/jsontapefile/<filepre>', jsontapefilelist),              # json tape file list handler)
    Rule('/request_account/<seq_of:things>', request_account),      # new account request
    Rule('/password_reset/<int:userid>/<token>', password_reset),   # account password reset request
    Rule('/login/<seq_of:things>', login),                          # login form
    Rule('/whoami/<seq_of:things>', whoami),                        # whoami
    Rule('/change_email/<seq_of:things>', change_email),            # change_password
    Rule('/change_password/<seq_of:things>', change_password),      # change_password
    Rule('/my_programs/<seq_of:things>', my_programs),              # my_programs
    Rule('/preview/<filenamegiven>', preview),                      # previews
    Rule('/preview/<filenamegiven>/<int:number>', preview),         # previews
    Rule('/num_previews/<filenamegiven>', num_previews),            # number of available previews related to the given file

    Rule('/console', console),
    Rule('/console/ingest_queue', console_ingest_queue),
    Rule('/console/ingest_errors', console_ingest_errors),
    Rule('/console/export_queue', console_export_queue),
    Rule('/queuestatus', queuestatus_summary),                      # Show some info on what's going on with the queues
    Rule('/queuestatus/json', queuestatus_update),                  # Show some info on what's going on with the queues
    Rule('/queuestatus/<qshortname>/<int:oid>', queuestatus_tb),    # Show some info on what's going on with the queues

    Rule('/miscfiles', miscfiles.miscfiles),                        # Miscellanea (Opaque files)
    Rule('/miscfiles/<int:handle>', miscfiles.miscfiles),           # Miscellanea (Opaque files)
    Rule('/miscfiles/validate_add', miscfiles.validate,             # Miscellanea (Opaque files)
         methods=['POST']),

    # Rules for "Miscfiles Plus" interfaces
    Rule('/miscfilesplus/', miscfilesplus.miscfilesplus),           # Enhanced Misc
    Rule('/miscfilesplus/browse/<collection>/',
         miscfilesplus.miscfilesplus),                              # MFP for Collection
    Rule('/miscfilesplus/browse/<collection>/<seq_of:folders>/',
         miscfilesplus.miscfilesplus),                              # MFP for Folder
    Rule('/miscfilesplus/download/<collection>/<seq_of:folders>/<filename>',
         miscfilesplus.get_file),                                   # MFP for File
    Rule('/miscfilesplus/download/<collection>/<filename>',
         miscfilesplus.get_file),                                   # MFP for File
    Rule('/miscfilesplus/downloadzip',
         miscfilesplus.download_zip),                                   # MFP for File
    Rule('/miscfilesplus/bulkaction',
         miscfilesplus.bulk_action),                                   # MFP for File
    Rule('/miscfilesplus/add_collection',
         miscfilesplus.add_collection),                             # MFP action to add a collection
    Rule('/miscfilesplus/add_folder', miscfilesplus.add_folder),    # MFP action to add a folder
    Rule('/miscfilesplus/upload_file', miscfilesplus.upload_file),  # MFP action to upload a file
    Rule('/miscfilesplus/delete/<collection>/<seq_of:path>/',
         miscfilesplus.delete_path),                                # MFP action to delete a folder or file
    Rule('/miscfilesplus/search/',
         miscfilesplus.search),  # MFP action to search

    Rule('/standardobs/<int:header_id>', standardobs),              # This is the standard star in observation server
    Rule('/upload_file/<filename>', upload_file,                    # The generic upload_file server
         methods=['POST']),
    Rule('/upload_processed_cal/<filename>',                        # The processed_cal upload server
         partial(upload_file, processed_cal=True),
         methods=['POST']),

    # This returns the fitsverify, mdreport or fullheader text from the database
    # you can give it either a diskfile_id or a filename
    Rule('/fitsverify/<thing>', report),
    Rule('/mdreport/<thing>', report),
    Rule('/fullheader/<thing>', report),

    Rule('/fitsverify', report, defaults=dict(thing=None)),
    Rule('/mdreport', report, defaults=dict(thing=None)),
    Rule('/fullheader', report, defaults=dict(thing=None)),

    Rule('/calibrations/<selection:selection>', calibrations),      # The calibrations handler
    Rule('/xmlfilelist/<selection:selection>', xmlfilelist),        # The xml and json file list handlers
    Rule('/jsonfilelist/<selection:selection>', jsonfilelist),
    Rule('/jsonfilenames/<selection:selection>', partial(jsonfilelist, fields={'name'})),
    Rule('/jsonsummary/<selection:selection>', jsonsummary,
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/jsonqastate/<selection:selection>', jsonqastate),
    Rule('/calmgr/<selection:selection>', xmlcalmgr),               # The calmgr handler, returning xml (legacy url)
    Rule('/xmlcalmgr/<selection:selection>', xmlcalmgr),            # The calmgr handler, returning xml
    Rule('/jsoncalmgr/<selection:selection>', jsoncalmgr),          # The calmgr handler, returning json
    Rule('/gmoscal/<selection:selection>', gmoscal_html),           # The GMOS twilight flat and bias report
    Rule('/gmoscaltwilightdetails', gmoscaltwilightdetails),        # The GMOS twilight flat and bias report
    Rule('/gmoscaltwilightfiles', gmoscaltwilightfiles),            # The GMOS twilight flat list of files
    Rule('/gmoscalbiasfiles/<selection:selection>', gmoscalbiasfiles),  # The GMOS bias list of files
    Rule('/programsobserved/<selection:selection>',                 # This is the projects observed feature
         progsobserved),

    # Group of URIs dealing with program/publication
    Rule('/programinfo/<program_id>', program_info),                # Displays data from a program
    Rule('/programinfojson/<program_id>', program_info_json),                # Displays data from a program
    Rule('/logcomments/<selection(SEL):selection>', log_comments),
    Rule('/programs/<selection:selection>', programs),
    # Obslogs get their own summary-like handler
    # Both actions use the same function, with 'sumtype' specifiying which
    # one of them, but aside from that, it's just a regular (req, selections)
    # We're using functools.partial here to pin sumtype's value
    Rule('/obslogs/<selection:selection>', partial(obslogs, sumtype='obslogs')),
    Rule('/associated_obslogs/<selection:selection>', partial(obslogs, sumtype='associated_obslogs')),

    # The GMOS twilight flat and bias report (JSON result)
    # The function here is the same as for 'gmoscal'. We're using partial for
    # the same reason as with obslogs (see above)
    Rule('/gmoscaljson/<selection:selection>', gmoscal_json),

    # Archive Search Form
    Rule('/searchform/<seq_of:things>', searchform,
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),

    # This is the header summary handler
    Rule('/summary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'summary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/diskfiles/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'diskfiles'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/ssummary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'ssummary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/lsummary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'lsummary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/searchresults/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'searchresults'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/associated_cals/<selection(SEL,NOLNK,BONLY):selection,links,body_only>', partial(summary, 'associated_cals'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/associated_cals_json/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
         partial(summary, 'associated_cals_json'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),

    # ORCID login handling/account setup
    Rule('/orcid', orcid, collect_qs_args=dict(code='code'), defaults=dict(code=None)),

    Rule('/rawfiles/<filename>', rawfiles),                      # This is the fits file server

    ],

    converters = {
        'selection': SelectionConverter,
        'seq_of':    SequenceConverter
    }
)

for bu in blocked_urls:
    url_map.add_forbidden(bu)


def get_route(routes):
    env = get_context().env
    return routes.match(env.uri, env.method)


def default_route(environ, start_response):
    resp = get_context().resp
    resp.set_content_type('text/plain')
    resp.append("This is the default page!\nYou're probably looking for something else")


def dispatch(endpoint, args):
    kw = {}
    for d in args:
        kw.update(d)
    return endpoint(**kw)


def unicode_to_string(uni):
    return uni.encode('utf-8') if isinstance(uni, str) else uni


def core_handler(environ, start_response):
    ctx = get_context()
    req, resp = ctx.req, ctx.resp
    resp.set_header('Cache-Control', 'no-cache')
    resp.set_header('Expired', '-1')

    route = get_route(url_map)
    if route is None:
        resp.client_error(Return.HTTP_NOT_FOUND, "Could not find the requested resource")
    else:
        dispatch(*route)
        return resp.respond(unicode_to_string)


class StaticServer(object):
    """
    Middleware class. An instance of StaticServer will intercept /static queries and
    return the static file (relative to certain root directory).

    Ideally, /static will be dealt with at a higher level. In that case, this doesn't
    introduce a significative overhead.
    """
    def __init__(self, application, root):
        self.app  = application
        self.root = root

    def __call__(self, environ, start_response):
        ctx = get_context()
        req, resp = ctx.req, ctx.resp

        uri = list(filter(len, req.env.uri.split('/')))
        if len(uri) > 1 and uri[0] == 'static':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(htmldocroot, '/'.join(uri[1:]))
                if mtype is not None:
                    resp.set_content_type(mtype)
                return resp.append(open(path, 'rb').read()).respond()
            except IOError:
                resp.client_error(Return.HTTP_FORBIDDEN)
        if len(uri) > 1 and uri[0] == 'help':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(htmldocroot, '/'.join(uri))
                if mtype is not None:
                    resp.set_content_type(mtype)
                return resp.append(open(path, 'rb').read()).respond()
            except IOError:
                resp.client_error(Return.HTTP_FORBIDDEN)
        return self.app(environ, start_response)


htmldocroot = os.path.join(os.path.dirname(__file__), '..', 'htmldocroot')
htmldocroot = os.getenv('HTML_DOC_ROOT', htmldocroot)
handle_with_static = StaticServer(core_handler, root=htmldocroot)


def handler(environ, start_response):
    ctx = get_context()
    try:
        return handle_with_static(environ, start_response)
    except RequestRedirect as e:
        return ctx.resp.respond(unicode_to_string)
    except ClientError as e:
        if e.annotate is not None:
            session = ctx.session
            annotationClass = e.annotate
            try:
                log = session.query(annotationClass).filter(annotationClass.usagelog_id == ctx.usagelog.id).one()
            except NoResultFound:
                log = annotationClass(ctx.usagelog)
            if hasattr(e, 'message'):
                log.add_note(e.message)
            session.add(log)
        return ctx.resp.respond(unicode_to_string)


application = ArchiveContextMiddleware(handler)


# Provide a basic WSGI server, in case we're testing or don't need any fancy
# container...
if __name__ == '__main__':
    import wsgiref.simple_server

    server = os.getenv('WSGI_DEV_HOST', 'localhost')
    port   = os.getenv('WSGI_DEV_PORT', '8090')

    try:
        httpd = wsgiref.simple_server.make_server(server, int(port), application)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nExiting after Ctrl-c")