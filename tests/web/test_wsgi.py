import pytest
import sqlalchemy.orm.exc as orm_exc
from urllib import urlencode
from StringIO import StringIO
from random import randint

from copy import deepcopy
from fits_storage.utils.web import get_context, Return, ClientError, RequestRedirect
from fits_storage.utils.web import WSGIRequest, WSGIResponse, ArchiveContextMiddleware
from fits_storage.utils.web import routing

# Monkey patch routing.Map to override the need for a context
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
                    raise RuntimeError("{}: HTTP Forbidden".format(path_info))
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
  'user2': {'gemini_archive_session': 'Jaab1A1SVbDOCjGpYOPsDElBorBt58JXWJMRcg2EYsDKd9PA8W8W5SCn/R6baUFXVGHbE2QCHHlbHG+yfwoTGQZDTQgqD4X1IA+WzlCSLBQnoej+1t8iK/tYqTyHnHr2FVJ3U+ijwFPmxloplcqa/fWO17SFLQB5GiLrPgsNouKgj8M9vAK/IyzpYY2nSdXTo038k2S/OWm8JDMPr6Qp+FIByfvP4cEMdL3nHcCu6PhQsKtc+fbSt14Ie4UjJ6uu1rqzJc1iBThD3PwnYyRjuLtE3eiO7otThhwhbIf4gZrdrTvByROrtr5l2G45GBigFqxc+0TatfiPjszTaQiK/A=='}
}

default_env = {
  'SERVER_NAME':    'test_server',
  'REMOTE_ADDR':    '127.0.0.1',
  'PATH_INFO':      '/',
  'CONTENT_TYPE':   'text/plain',
  'QUERY_STRING':   '',
  'REQUEST_METHOD': 'GET',
  'HTTP_COOKIE':    {},
  'wsgi.output':    None,
  'wsgi.input':     None,
  'wsgi.errors':    None,
}

def get_env(uri, cookies = None, post=False, data=None):
    env = deepcopy(default_env)
    env['PATH_INFO'] = uri
    if cookies: env['HTTP_COOKIE'] = cookies
    if data:
        post = True
        if isinstance(data, dict):
            enc = urlencode(data)
            env.update({
                'CONTENT_TYPE':   'application/x-www-form-urlencoded',
                'CONTENT_LENGTH': str(len(enc)),
                'wsgi.input': StringIO(enc)
            })
        else:
            env.update({
                'CONTENT_TYPE': 'text/plain',
                'CONTENT_LENGTH': str(len(data)),
                'wsgi.input': StringIO(str(data))
            })
    if post: env['REQUEST_METHOD'] = 'POST'

    return env

class Fixture(object):
    cases = (
        (get_env('/user_list'),
         "You don't appear to be logged in as a Gemini Staff user"),
        (get_env('/user_list', cookies=cookies['user1']),
         ("<td>user1<td>User 1<td>unknown1@gemini.edu",
          "<td>user2<td>User 2<td>unknown2@gemini.edu")),
        (get_env('/content'),
         ("<p>Total number of files: 1,388",
          "<p>Total file storage size: 6.85 GB",
          "<p>Total FITS data size: 23.51 GB")),
        (get_env('/usagereport', data={'start': '2016-02-10', 'end': '2016-02-12'}),
          (ClientError, "You need to be logged in to access this resource")),
        (get_env('/usagereport', data={'start': '2016-02-10', 'end': '2016-02-12'},
                                 cookies=cookies['user1']),
         ('<td><a target="_blank" href="/usagedetails/134843">134843</a>',
          '<td><a target="_blank" href="/usagedetails/134840">134840</a>',
          '<td><a target="_blank" href="/usagedetails/134701">134701</a>')),
        (get_env('/stats'),
         ('<li>Present Rows: 1388 (99.14%)',
          '<li>Total present size: 7358825260 bytes (6.85 GB)',
          '<li>S20150917S0011.fits: 2015-09-16 09:46:06.687962-10:00')),
    )

    def get_route(self, env):
        return url_map.match(env['PATH_INFO'], env['REQUEST_METHOD'])

    def __iter__(self):
        for env, data in Fixture.cases:
            route = self.get_route(env)
            yield route, data, env

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
        print obj
    return write

def run_web_test(fn, env):
    tm = WebTestMiddleware(fn)
    ArchiveContextMiddleware(tm)(env, start_response)

    return tm

@pytest.mark.usefixtures("min_rollback")
@pytest.mark.parametrize("route,expected,env", Fixture())
def test_user_list(min_session, route, expected, env):
    if isinstance(expected, tuple) and expected[0] in (ClientError, RequestRedirect):
        excep, message = expected
        with pytest.raises(excep) as excinfo:
            run_web_test(route, env)
        assert message in excinfo.value
    else:
        tm = run_web_test(route, env)

        r = ''.join(tm.resp)
        if isinstance(expected, (str, unicode)):
            fixtures = (expected,)
        else:
            fixtures = expected
        for f in fixtures:
            assert (f in r) == True
