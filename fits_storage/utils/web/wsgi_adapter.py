from . import adapter
from .adapter import Return, RequestRedirect, ClientError
from ...fits_storage_config import upload_staging_path
from ...orm import session_scope
from wsgiref.handlers import SimpleHandler
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref import util as wutil
from cgi import escape, FieldStorage

import Cookie
from contextlib import contextmanager
from functools import wraps

class Environment(object):
    def __init__(self, env):
        self._env = env

    def __getitem__(self, item):
        return self._env[item]

    def __contains__(self, item):
        return item in self._env

    @property
    def server_hostname(self):
        return self._env['SERVER_NAME']

    @property
    def remote_host(self):
        try:
            return self._env['HTTP_X_FORWARDED_FOR'].split(',')[-1].strip()
        except KeyError:
            return self.remote_ip

    @property
    def uri(self):
        return self.unparsed_uri

    @property
    def unparsed_uri(self):
        return self._env.get('PATH_INFO', '')

    @property
    def remote_ip(self):
        return self._env['REMOTE_ADDR']

    @property
    def method(self):
        return self._env['REQUEST_METHOD']

    @property
    def cookies(self):
        return Cookie.SimpleCookie(self._env['HTTP_COOKIE'])

class Request(adapter.Request):
    def __init__(self, session, wsgienv):
        super(Request, self).__init__(session)

        self._env     = Environment(wsgienv)
        self._fields  = None

    @property
    def input(self):
        return self._env['wsgi.input']

    @property
    def env(self):
        return self._env

    @property
    def raw_data(self):
        return self.input.read()

    @property
    def json(self):
        return json.loads(self.raw_data)

    def get_header_value(self, header_name):
        return self._env[header_name]

    def contains_header(self, header_name):
        return header_name in self._env

    def get_cookie_value(self, key):
        return self._env.cookies[key].value

    def log(self, *args, **kw):
        try:
            print >> self._env['wsgi_errors'], args[0]
            return True
        except IndexError:
            return False

    def get_form_data(self, large_file=False):
        form_data = FieldStorage(self.input, environ=self._env)

        return form_data

BUFFSIZE = 262144

status_message = {
    Return.HTTP_OK:                  'OK',
    Return.HTTP_MOVED_PERMANENTLY:   'Moved Permanently',
    Return.HTTP_FOUND:               'Found',
    Return.HTTP_SEE_OTHER:           'See Other',
    Return.HTTP_NOT_MODIFIED:        'Not Modified',
    Return.HTTP_NOT_FOUND:           'Not Found',
    Return.HTTP_FORBIDDEN:           'Access Forbidden for This Resource',
    Return.HTTP_METHOD_NOT_ALLOWED:  'The Method Used to Access This Resource Is Not Allowed',
    Return.HTTP_NOT_ACCEPTABLE:      'The Returned Content Is Not Acceptable for the Client',
    Return.HTTP_NOT_IMPLEMENTED:     'Method Not Implemented',
    Return.HTTP_SERVICE_UNAVAILABLE: 'The Service Is Currently Unavailable',
    Return.HTTP_BAD_REQUEST:         'The Server Received a Bad Request -Probably Malformed JSON'
}

redirect_template = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<title>Redirecting...</title>
<h1>Redirecting...</h1>
<p>You should be redirected automatically to target URL: <a href="{location}">{display_location}</a>.  If not click the link."""

template_4xx = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<title>{code} {message}...</title>
<h1>{code} {message}...</h1>"""

def only_if_not_started_response(fn):
    @wraps(fn)
    def wrapper(self, *args, **kw):
        if self._started_response:
            # TODO: Look for a more sensible exception, to deal with the problem upwards.
            raise RuntimeError("Asked to send a non-2xx code, but the response started already")
        return fn(self, *args, **kw)
    return wrapper

class Response(adapter.Response):
    def __init__(self, session, wsgienv, start_response):
        super(Response, self).__init__(session)

        self._env = wsgienv
        self._sr  = start_response
        self._bytes_sent = 0
        self._cookies_to_send = Cookie.SimpleCookie()
        self.make_empty()
        self._started_response = False

    def respond(self, filter = None):
        self.start_response()
        if filter:
            for k in self:
                yield filter(k)
        else:
            for k in self:
                yield k

    def __iter__(self):
        for k in self._content:
            yield k

    @property
    def bytes_sent(self):
        return self._bytes_sent

    def start_response(self):
        if not self._started_response:
            self._started_response
            self._sr('{} {}'.format(self.status, status_message.get(self.status, 'Error')),
                     [('Content-Type', self._content_type)] + self._headers[:])
        return self

    def expire_cookie(self, name):
        self.set_cookie(name, expires=time.time())
        return self

    def set_cookie(self, name, value='', **kw):
        raise NotImplementedError("This is not implemented yet")
        # Cookie.add_cookie(self._req, name, value, **kw)
        # self._cookies_to_send[name] = value
        # for attr, val in kw.iteritems():
        #     self._cookies_to_send[name][attr] = val

    def set_content_type(self, content_type):
        self._content_type = content_type
        return self

    def set_header(self, name, value):
        self._headers.append((name, value))
        return self

    def append(self, string):
        self._content.append(string)
        return self

    def append_iterable(self, it):
        self.content.append(it)
        return self

    def append_json(self, obj, **kw):
        raise NotImplementedError("This is not implemented yet")
        # json.dump(obj, self._req, **kw)

    def sendfile(self, path):
        raise NotImplementedError("This is not implemented yet")
        # self._req.sendfile(path)

    def sendfile_obj(self, fp):
        raise NotImplementedError("This is not implemented yet")
        # while True:
        #     n = fp.read(BUFFSIZE)
        #     if not n:
        #         break
        #     self._req.write(n)

    @contextmanager
    def tarfile(self, name, **kw):
        raise NotImplementedError("This is not implemented yet")
        #self.set_header('Content-Disposition', 'attachment; filename="{}"'.format(name))
        #tar = tarfile.open(name=name, fileobj=self._req, **kw)

        #try:
        #    yield tar
        #finally:
        #    tar.close()
        #    self._req.flush()

    def make_empty(self):
        self._content = []
        self._content_type = 'text/plain'
        self._headers = []
        self.status = Return.HTTP_OK

    @only_if_not_started_response
    def redirect_to(self, url, **kw):
        self.make_empty()
        self.set_content_type('text/html')
        # Set the status to 'code' if passed as an argument, else use 302 FOUND as default
        self.status = (kw['code'] if 'code' in kw else Return.HTTP_FOUND)

        display_location=escape(url)
        self.append(redirect_template.format(location=escape(url), display_location=display_location))
        self.set_header('Location', url)

        raise RequestRedirect()

    @only_if_not_started_response
    def client_error(self, code):
        """
        Helper to raise 4xx "Client Error" exceptions
        """
        self.make_empty()
        self.set_content_type('text/html')
        self.status = code
        self.append(template_4xx.format(code=code, message=status_message[code]))

        raise ClientError(code)

from ...orm.usagelog import UsageLog

class ArchiveHandler(SimpleHandler):
    def run(self, application):
        """Invoke the application"""
        self.status = '500 Internal Error'
        ctx = adapter.Context()
        try:
            self.setup_environ()
            with session_scope() as session:
                try:
                    request = Request(session, self.environ)
                    response = Response(session, self.environ, self.start_response)
                    ctx.setContent(request, response)

                    usagelog = UsageLog(ctx)
                    ctx.usagelog = usagelog
                    ctx.session = session

                    try:
                        ctx.usagelog.user_id = request.user.id
                    except AttributeError:
                        # No user defined
                        pass
                    session.add(usagelog)
                    session.commit()

                    self.result = application(self.environ, self.start_response)
                    self.finish_response()
                finally:
                    session.commit()
                    ctx.usagelog.set_finals(ctx)
                    session.commit()
                    session.close()
        except:
            try:
                self.handle_error()
            except:
                # If we get an error when handling the error, just give up!
                self.close()
                raise # And let the actual server figure out...
        finally:
            ctx.invalidate()

    def close(self):
        """Copied from wsgiref.simple_server.ServerHandler"""
        try:
            self.request_handler.log_request(
                self.status.split(' ',1)[0], self.bytes_sent
            )
        finally:
            SimpleHandler.close(self)

class ArchiveWSGIRequestHandler(WSGIRequestHandler):
    def handle(self):
        """Handle a single HTTP request"""

        self.raw_requestline = self.rfile.readline()
        if not self.parse_request(): # An error code has been sent, just exit
            return

        handler = ArchiveHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())
