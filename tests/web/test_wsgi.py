import pytest
import sqlalchemy.orm.exc as orm_exc
from urllib.parse import urlencode
from io import StringIO, BytesIO
from random import randint
import json
import os

from copy import deepcopy

from fits_storage import fits_storage_config
from fits_storage.utils.web import get_context, Return, ClientError, RequestRedirect
from fits_storage.utils.web import WSGIRequest, WSGIResponse, ArchiveContextMiddleware
from fits_storage.utils.web import routing

from tests.file_helper import ensure_file


# URLs that are blocked in the main archive. Use them to simulate the real thing
# We monkeypatch them into fits_storage_config so that they're picked up by the
# wsgihandler module when imported
#from fits_storage import fits_storage_config
#fits_storage_config.blocked_urls = [
#    'debug', 'fileontape', 'qareport', 'qametrics', 'qaforgui', 'tape',
#    'tapewrite', 'tapefile', 'taperead', 'xmltape', 'gmoscal', 'update_headers',
#    'ingest_files'
#]

# Monkeypatch routing.Map to override the need for a context
class MapTester(routing.Map):
    def match(self, path_info, method=None):
        found = False
        for rule in self._rules:
            m = rule.match(path_info)
            if m is not None:
                found = True
                if method is not None and rule.methods and method not in rule.methods:
                    # Most probably we want to keep track of this, to raise an exception
                    # if there was a match but no compatible method
                    continue
                if self.is_forbidden(rule.this):
                    return (Return.HTTP_FORBIDDEN, "{}: HTTP Forbidden".format(path_info))
                elif rule.redirect_to is not None:
                    return (Return.HTTP_FOUND, rule.redirect_to)
                return rule.action, m

        # If we get here and found is True, then it means that there's a route, but the
        # method we used was not allowed
        if found:
            return (Return.HTTP_METHOD_NOT_ALLOWED, None)

routing.Map = MapTester

from fits_storage.wsgihandler import url_map, dispatch

from fits_storage.web.user import user_list

cookies = {
  'user1': {'gemini_archive_session': 'oYgq13TfqRt+x1xrNnGNMZJIMZj+p1GyIWV/Ebm3/BsD05dCf5KKQOvtrGim9YG5XgsVCn8sDSBeaBHuh1I6A9st5CLr5auN9tYOlLzCFo15i64RUVfByFmqaxgJuHHAim4HBKdOlq/Mo4YHhMNAQKgUJnkEj27xoL6+YXSsNfmEDzB/PmmNzc+jz3sMCYuxt/NVftEo0FB1xk3xvCj5kkkE9DjRiSibtaD5EIluv2nYkmaSIxThfqpilj9UJhg4uc3pLN2I+R15IWa3h8HskqyjBL3tiq0paVWDv8BoOgeBwK24Igw0Vnn8vQQ8Ys6a4DZ2c84YaIjXEaL26VSw5A=='},
  'user2': {'gemini_archive_session': 'Jaab1A1SVbDOCjGpYOPsDElBorBt58JXWJMRcg2EYsDKd9PA8W8W5SCn/R6baUFXVGHbE2QCHHlbHG+yfwoTGQZDTQgqD4X1IA+WzlCSLBQnoej+1t8iK/tYqTyHnHr2FVJ3U+ijwFPmxloplcqa/fWO17SFLQB5GiLrPgsNouKgj8M9vAK/IyzpYY2nSdXTo038k2S/OWm8JDMPr6Qp+FIByfvP4cEMdL3nHcCu6PhQsKtc+fbSt14Ie4UjJ6uu1rqzJc1iBThD3PwnYyRjuLtE3eiO7otThhwhbIf4gZrdrTvByROrtr5l2G45GBigFqxc+0TatfiPjszTaQiK/A=='},
  'fits': {'gemini_fits_authorization': fits_storage_config.magic_download_cookie}
}

default_env = {
  'SERVER_NAME':       'test_server',
  'REMOTE_ADDR':       '127.0.0.1',
  'PATH_INFO':         '/',
  'CONTENT_TYPE':      'text/plain',
  'QUERY_STRING':      '',
  'REQUEST_METHOD':    'GET',
  'HTTP_COOKIE':       {},
  'wsgi.input':        None,
  'wsgi.errors':       StringIO(),
  'wsgi.multithread':  False,
  'wsgi.multiprocess': False,
  'wsgi.run_once':     True,
  'wsgi.version':      (1, 0),
  'wsgi.url_scheme':   'http'
}

class Fixture(object):
    def __init__(self, uri, cookies=None, post=False, data=None, retcode=200, cases=(), exception=None, json=False,
                 ensure=None):
        self.uri     = uri
        self.cookies = cookies
        self.json    = json
        self.data    = data
        self.post    = (post if data is None else True)
        self.status  = retcode
        if isinstance(cases, str):
            self.cases = (cases,)
        else:
            self.cases = cases
        self.exc     = exception
        if ensure is not None:
            for filename in ensure:
                ensure_file(filename)

    def get_env(self):
        env = deepcopy(default_env)
        env['PATH_INFO'] = self.uri
        if self.cookies:
            env['HTTP_COOKIE'] = self.cookies

        pytest_server = os.getenv("PYTEST_SERVER", None)
        if pytest_server is not None:
            if pytest_server.lower().startswith("http"):
                pytest_server = pytest_server[pytest_server.index('/')+2:]
            env['REMOTE_ADDR'] = pytest_server

        post = self.post
        if self.data is not None:
            if self.json:
                enc = json.dumps(self.data).encode('utf-8')
                env.update({
                    'CONTENT_TYPE':   'application/json',
                    'CONTENT_LENGTH': str(len(enc)),
                    'wsgi.input':     BytesIO(enc)
                })
            elif isinstance(self.data, dict):
                enc = urlencode(self.data).encode('utf-8')
                env.update({
                    'CONTENT_TYPE':   'application/x-www-form-urlencoded',
                    'CONTENT_LENGTH': str(len(enc)),
                    'wsgi.input':     BytesIO(enc)
                })
            else:
                env.update({
                    'CONTENT_TYPE':   'text/plain',
                    'CONTENT_LENGTH': str(len(self.data)),
                    'wsgi.input':     BytesIO(self.data)
                })

        if post:
            env['REQUEST_METHOD'] = 'POST'
            if self.data is None:
                env.update({
                    'CONTENT_TYPE':   'text/plain',
                    'CONTENT_LENGTH': '0',
                    'wsgi.input':     StringIO('')
                })

        return env

    def __repr__(self):
        return "Fixture({!r}, method={})".format(self.uri, ('POST' if self.post else 'GET'))

class FixtureIter(object):
    def __init__(self, cases):
        self.cases = cases

    def get_route(self, env):
        return url_map.match(env['PATH_INFO'], env['REQUEST_METHOD'])

    def __iter__(self):
        for fix in self.cases:
            route = self.get_route(fix.get_env())
            yield route, fix

class WebTestMiddleware(object):
    def __init__(self, tested_case):
        self.tested_case = tested_case
        self.resp = None

    def __call__(self, environ, start_response):
        ctx = get_context()
        self.resp = ctx.resp
        dispatch(*self.tested_case)

def start_response(*args):
    def write(obj):
        print(obj)
    return write

def run_web_test(fn, env):
    tm = WebTestMiddleware(fn)
    ArchiveContextMiddleware(tm)(env, start_response)

    return tm

DEBUGGING=False
NORTH=True

if NORTH:
    file_future_release='N20150708S0102.fits'
else:
    file_future_release='S20150913S0044.fits'

fixtures = (
    # test /user_list, first with anonymous user, then with logged-in user
    Fixture('/user_list', cases="You don't appear to be logged in as a Gemini Staff user"),
    Fixture('/user_list', cookies=cookies['user1'],
            cases=("<td>user1<td>User 1<td>unknown1@gemini.edu",
                   "<td>user2<td>User 2<td>unknown2@gemini.edu")),
    # test /usagereport, first with anonymous user, then with logged-in user
    Fixture('/usagereport', data={'start': '2016-02-10', 'end': '2016-02-12'},
            exception = (ClientError, "You need to be logged in to access this resource")),
    Fixture('/usagereport', data={'start': '2016-02-10', 'end': '2016-02-12'}, cookies=cookies['user1']), #,
    #O TODO fix and add back in somehow
            # cases=('<td><a target="_blank" href="/usagedetails/134843">134843</a>',
            #        '<td><a target="_blank" href="/usagedetails/134840">134840</a>',
            #        '<td><a target="_blank" href="/usagedetails/134701">134701</a>')),
    # test /content, no need for user

    #O TODO need records in the database or this call errors out entirely
    # Fixture('/content',
    #         cases=("<p>Total number of files: 1,388",
    #                "<p>Total file storage size: 6.85 GB",
    #                "<p>Total FITS data size: 23.51 GB")),

    #O TODO need records in the database
    # Test /stats, no need for user
    # Fixture('/stats',
    #         cases=('<li>Present Rows: 1388 (99.14%)',
    #                '<li>Total present size: 7358825260 bytes (6.85 GB)',
    #                '<li>S20150917S0011.fits: 2015-09-16 09:46:06.687962-10:00')),

    # Test /qareport, first using GET, then POST
    Fixture('/qareport', retcode=Return.HTTP_METHOD_NOT_ALLOWED),
    Fixture('/qareport', json=True, data=[]),
    # Test /usagestats, both with anonymous and registered user
    Fixture('/usagestats',
            exception = (ClientError, "You need to be logged in to access this resource")),
    Fixture('/usagestats', cookies=cookies['user2'],
            cases='<h1>Usage Statistics'),
    # Test /taperead
    Fixture('/xmltape', cases='<?xml version="1.0"?>\n<on_tape>\n\n</on_tape>'),
    # Test /taperead
    Fixture('/taperead', cases='<h1>FITS Storage taperead information'),
    # Test /notification without and with authentication
    Fixture('/notification', exception=(ClientError, 'You need to be logged in to access this resource')),
    Fixture('/notification', cookies=cookies['user2'],
            cases='<title>FITS Storage new data email notification list'),
    # Test /import_odb_notifications
    Fixture('/import_odb_notifications', retcode=Return.HTTP_METHOD_NOT_ALLOWED),
    Fixture('/import_odb_notifications', post=True, cookies=cookies['fits'],
            exception=(ClientError, '<!-- The content sent is not valid XML -->')),
    # Test /logout
    Fixture('/logout', cases='You are sucessfully logged out of the Gemini Archive.'),
    Fixture('/logout', cookies=cookies['user2'],
            cases='You are sucessfully logged out of the Gemini Archive.'),
    # Test /curation
    Fixture('/curation',
            cases=('<h2>Duplicate Canonical DiskFiles:',
                   'None found.')),
    # Test /nameresolver
    Fixture('/nameresolver', retcode=404),
    Fixture('/nameresolver/simbad/m31',
            cases=('{"success": true, "ra": 10.68470833, "dec": 41.26875}')),
    # Test /fileontape
    Fixture('/fileontape', retcode=404),
    Fixture('/fileontape/foobar', cases='<?xml version="1.0" ?>\n<file_list>\n</file_list>'),

    # Test /file
    Fixture('/file', retcode=404), # Not found because the URL matches no route
    Fixture('/file/foobar', retcode=404), # Not found because the file does not exist

    #O TODO come up with a fresh example of this
    # Fixture('/file/{}'.format(file_future_release), # Release date in the future - this test will work until 2017-01
    #         exception=(ClientError, 'Not enough privileges to download this content')),

    #O TODO come up with a fresh example of this
    # Fixture('/file/S20150901S0661.fits', # Propietary coords
    #         exception=(ClientError, 'Not enough privileges to download this content')),
    #O TODO come up with a fresh example of this
    # Fixture('/file/N20111115S0250.fits',
    #         cases="SIMPLE  =                    T / file does conform to FITS standard"),

    #O TODO come up with fresh example (per earlier)
    # Fixture('/file/{}'.format(file_future_release), # Release date in the future - this test will work until 2017-01
    #         cookies=cookies['user2'],
    #         cases="SIMPLE  =                    T / file does conform to FITS standard"),

    #O TODO come up with a fresh example
    # Fixture('/file/S20150901S0661.fits', # Propietary coords
    #         retcode=(404 if NORTH else 200),
    #         cookies=cookies['user2'],
    #         cases="SIMPLE  =                    T / file does conform to FITS standard",
    #         exception=(None if not NORTH else
    #                    (ClientError, 'This was unexpected. Please, inform the administrators.'))),

    # Test /download (POST and GET) - just make sure that they return something
    # TODO come up with a fresh example (per earlier as well)
    # Fixture('/download', data={'files': ['N20111115S0250.fits', file_future_release]}),
    # Fixture('/download/N20111115S0250.fits'),

    # Test /qametrics
    Fixture('/qametrics', cases=""),
    Fixture('/qametrics/iq/sb',
            cases="#Datalabel, filename, detector, filter, utdatetime, Nsamples, FWHM, FWHM_std, isoFWHM, isoFWHM_std, EE50d, EE50d_std"),

    # Test /qaforgui
    Fixture('/qaforgui', retcode=404),
    Fixture('/qaforgui/20151201', cases="[]"),

    # Test /usagedetails
    Fixture('/usagedetails', retcode=404),
    Fixture('/usagedetails/196',
            exception=(ClientError, "You need to be logged in to access this resource")),
    # TODO come up with a fresh example
    # Fixture('/usagedetails/196', cookies=cookies['user2'],
    #         cases="<tr><td>HTTP status:<td>200 (OK)</tr>"),

    # Test /downloadlog
    Fixture('/downloadlog',
            exception=(ClientError, "You need to be logged in to access this resource")),
    Fixture('/downloadlog', cookies=cookies['user2'],
            cases='Please, provide at least one filename pattern for the query'),
    # TODO come up with a fresh example
    # Fixture('/downloadlog/N20120825S05', cookies=cookies['user2'],
    #         cases="<td>128.171.188.44\n    <td>2015-07-16 00:06:28.113801\n    <td>200 (OK)\n  </tr>"),
    Fixture('/my_programs', cookies=cookies['user1'], cases="SPARKYTHEGECKO"),
    Fixture('/my_programs', cookies=cookies['user1'], data={'program_id': 'SPARKYTHEGECKO'}, cases="SPARKYTHEGECKO"),

    Fixture('/gmoscaljson/GN-CAL20200214-2-001', ensure=["N20200214S1347.fits", ],
            cases=('"twilight_flats": []',
                   '1x1',
                   '"Full Frame": 1')),
    Fixture('/gmoscal/GN-CAL20200214-2-001', ensure=["N20200214S1347.fits", ],
            cases=('Imaging Twilight', )),
)

@pytest.mark.usefixtures("min_rollback")
@pytest.mark.parametrize("route,expected", FixtureIter(fixtures))
def test_wsgi(min_session, route, expected):
    if route is None:
        assert expected.status == 404
    elif isinstance(route[0], int):
        assert expected.status == route[0]
    else:
        env = expected.get_env()
        if expected.exc:
            excep, message = expected.exc
            with pytest.raises(excep) as excinfo:
                run_web_test(route, env)
            assert message in str(excinfo.value)
        else:
            tm = run_web_test(route, env)

            assert expected.status == tm.resp.status
            if expected.cases:
                str_resp = ''
                for el in tm.resp:
                    if isinstance(el, str):
                        str_resp = str_resp + el
                    else:
                        str_resp = str_resp + el.decode('utf-8')
                # r = b''.join(tm.resp)
                # r = r.decode('utf-8')
                for f in expected.cases:
                    try:
                       assert f in str_resp
                    except AssertionError:
                        if DEBUGGING:
                            print(str_resp)
                        raise
