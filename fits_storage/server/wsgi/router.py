"""
This module contains the routing implementation that uses the routes.py
functions. This used to be bundled in the wsgihandler.py module
"""
from functools import partial

from fits_storage.server.wsgi.routing import Map, Rule
from fits_storage.server.wsgi.context import get_context

from .debug import debugmessage

from fits_storage.web.statistics import stats, content
from fits_storage.web.curationreport import curation_report

from fits_storage.web.logreports import \
    usagereport, usagedetails, downloadlog, usagestats

from fits_storage.web.summary import summary
from fits_storage.web.diskfilereports import report

from fits_storage.web.searchform import searchform, nameresolver

from fits_storage.web.user import whoami, login, logout, request_account, \
    request_password_reset, change_password, change_email, password_reset, \
    user_list, staff_access, admin_change_email, admin_change_password, \
    admin_file_permissions

from fits_storage.web.userprogram import my_programs

from fits_storage.web.tapestuff import tape, tapewrite, tapefile, \
    jsontapefilelist, taperead, fileontape

from .routing import SequenceConverter, SelectionConverter

from fits_storage.config import get_config
fsc = get_config()

url_map = Map([
    # Queries to the root should redirect to a sensible page
    Rule('/', redirect_to=('/searchform' if fsc.is_archive else '/static/usage.html')),

    # Debugging and testing
    Rule('/debug', debugmessage),

    # Database statistics etc
    Rule('/content', content),
    Rule('/stats', stats),
    Rule('/curation', curation_report),

    # Log queries and reports
    Rule('/usagereport', usagereport),
    Rule('/usagestats', usagestats),
    Rule('/usagedetails/<int:ulid>', usagedetails),
    Rule('/downloadlog/<seq_of:patterns>', downloadlog),

    # User accounts
    Rule('/whoami/<seq_of:things>', whoami),
    Rule('/my_programs/<seq_of:things>', my_programs),
    Rule('/login/<seq_of:things>', login),
    Rule('/logout', logout),
    Rule('/request_account/<seq_of:things>', request_account),
    Rule('/request_password_reset', request_password_reset),
    Rule('/password_reset/<int:userid>/<token>', password_reset),
    Rule('/change_password/<seq_of:things>', change_password),
    Rule('/change_email/<seq_of:things>', change_email),
    Rule('/user_list', user_list),
    Rule('/staff_access', staff_access),
    Rule('/admin_change_email', admin_change_email),
    Rule('/admin_change_password', admin_change_password),
    Rule('/admin_file_permissions', admin_file_permissions),

    # Tape stuff
    Rule('/tape', tape),
    Rule('/tape/<search>', tape),
    Rule('/tapewrite', tapewrite),
    Rule('/tapewrite/<label>', tapewrite),
    Rule('/tapefile/<int:tapewrite_id>', tapefile),
    Rule('/jsontapefile/<filepre>', jsontapefilelist),
    Rule('/taperead', taperead),
    Rule('/fileontape/<filename>', fileontape),

    # Diskfile Reports - you can give these either a diskfile_id or a filename
    Rule('/fitsverify/<thing>', report),
    Rule('/mdreport/<thing>', report),
    Rule('/fullheader/<thing>', report),

    # Archive Search Form, summaries etc
    Rule('/searchform/<seq_of:things>', searchform,
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),

    Rule('/summary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
         partial(summary, 'summary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/diskfiles/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
         partial(summary, 'diskfiles'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/ssummary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
         partial(summary, 'ssummary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule('/lsummary/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
         partial(summary, 'lsummary'),
         collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule(
        '/searchresults/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
        partial(summary, 'searchresults'),
        collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule(
        '/associated_cals/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
        partial(summary, 'associated_cals'),
        collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    Rule(
        '/associated_cals_json/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
        partial(summary, 'associated_cals_json'),
        collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),

    Rule('/nameresolver/<resolver>/<target>', nameresolver),

    # File server and downloads
    # Rule('/file/<filenamegiven>', fileserver),
    # Rule('/download/', download_post, methods=['POST']),
    # Rule('/download/<selection(SEL,ASSOC):selection,associated_calibrations>',
    #      download, methods=['GET']),

    # Previews
    # Rule('/preview/<filenamegiven>', preview),
    # Rule('/preview/<filenamegiven>/<int:number>', preview),
    # Rule('/num_previews/<filenamegiven>', num_previews),

    # QA metrics
    # Rule('/qareport', qareport, methods=['POST']),
    # Rule('/qametrics/<seq_of(iq,zq,sb,pe):metrics>', qametrics),
    # Rule('/qaforgui/<date>', qaforgui),

    # Notifications
    # Rule('/notification', notification),
    # Rule('/import_odb_notifications', import_odb_notifications, methods=['POST']),


    # Rule('/update_headers', update_headers, methods=['POST']),      # JSON RPC dispatcher
    # Rule('/ingest_files', ingest_files, methods=['POST']),          # JSON RPC dispatcher
    # Rule('/ingest_programs', ingest_programs, methods=['POST']),    # JSON RPC dispatcher
    # Rule('/ingest_publications', ingest_publications,
    #      methods=['POST']),                                          # JSON RPC dispatcher
    # Rule('/publication/ads/<bibcode>', publication_ads),            # Publication ADS Info
    # Rule('/list_publications', list_publications),                  # Publication Bibcode/Link List


    # Rule('/miscfiles', miscfiles.miscfiles),                        # Miscellanea (Opaque files)
    # Rule('/miscfiles/<int:handle>', miscfiles.miscfiles),           # Miscellanea (Opaque files)
    # Rule('/miscfiles/validate_add', miscfiles.validate,             # Miscellanea (Opaque files)
    #      methods=['POST']),
    #
    # Rule('/standardobs/<int:header_id>', standardobs),              # This is the standard star in observation server
    # Rule('/upload_file/<filename>', upload_file,                    # The generic upload_file server
    #      methods=['POST']),
    # Rule('/upload_processed_cal/<filename>',                        # The processed_cal upload server
    #      partial(upload_file, processed_cal=True),
    #      methods=['POST']),
    #

    #
    # Rule('/calibrations/<selection:selection>', calibrations),      # The calibrations handler
    # Rule('/xmlfilelist/<selection:selection>', xmlfilelist),        # The xml and json file list handlers
    # Rule('/jsonfilelist/<selection:selection>', jsonfilelist),
    # Rule('/jsonfilenames/<selection:selection>', partial(jsonfilelist, fields={'name'})),
    # Rule('/jsonsummary/<selection:selection>', jsonsummary,
    #      collect_qs_args=dict(orderby='orderby'), defaults=dict(orderby=None)),
    # Rule('/jsonqastate/<selection:selection>', jsonqastate),
    # Rule('/calmgr/<selection:selection>', xmlcalmgr),               # The calmgr handler, returning xml (legacy url)
    # Rule('/xmlcalmgr/<selection:selection>', xmlcalmgr),            # The calmgr handler, returning xml
    # Rule('/jsoncalmgr/<selection:selection>', jsoncalmgr),          # The calmgr handler, returning json
    # Rule('/gmoscal/<selection:selection>', gmoscal_html),           # The GMOS twilight flat and bias report
    # Rule('/gmoscaltwilightdetails', gmoscaltwilightdetails),        # The GMOS twilight flat and bias report
    # Rule('/gmoscaltwilightfiles', gmoscaltwilightfiles),            # The GMOS twilight flat list of files
    # Rule('/gmoscalbiasfiles/<selection:selection>', gmoscalbiasfiles),  # The GMOS bias list of files
    # Rule('/programsobserved/<selection:selection>',                 # This is the projects observed feature
    #      progsobserved),
    #
    # # Group of URIs dealing with program/publication
    # Rule('/programinfo/<program_id>', program_info),                # Displays data from a program
    # Rule('/programinfojson/<program_id>', program_info_json),                # Displays data from a program
    # Rule('/logcomments/<selection(SEL):selection>', log_comments),
    # Rule('/programs/<selection:selection>', programs),
    # # Obslogs get their own summary-like handler
    # # Both actions use the same function, with 'sumtype' specifiying which
    # # one of them, but aside from that, it's just a regular (req, selections)
    # # We're using functools.partial here to pin sumtype's value
    # Rule('/obslogs/<selection:selection>', partial(obslogs, sumtype='obslogs')),
    # Rule('/associated_obslogs/<selection:selection>', partial(obslogs, sumtype='associated_obslogs')),
    #
    # # The GMOS twilight flat and bias report (JSON result)
    # # The function here is the same as for 'gmoscal'. We're using partial for
    # # the same reason as with obslogs (see above)
    # Rule('/gmoscaljson/<selection:selection>', gmoscal_json),
    #

    # # ORCID login handling/account setup
    # Rule('/orcid', orcid, collect_qs_args=dict(code='code'), defaults=dict(code=None)),
    #
    # Rule('/rawfiles/<filename>', rawfiles),                      # This is the fits file server

    ],

    converters = {
        'selection': SelectionConverter,
        'seq_of':    SequenceConverter
    }
)

for bu in fsc.blocked_urls:
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
