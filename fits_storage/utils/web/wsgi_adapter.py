from . import adapter
from .adapter import Return, RequestRedirect, ClientError
from ...fits_storage_config import upload_staging_path
from ...orm import sessionfactory
from ...web import templating
from wsgiref.handlers import SimpleHandler
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref import util as wutil
from cgi import escape, FieldStorage
import http.cookies
from datetime import datetime
import json
import tarfile
import sys

# Boilerplate object. Maybe later we'll add something else to it?
def Uploadedfile(object):
    def __init__(self, name):
        self.name = name

class ItemizedFieldStorage(FieldStorage):
    def __init__(self, fp, environ):
        FieldStorage.__init__(self, fp, environ=environ)
        if self.filename is None:
            self.uploaded_file = None
        else:
            self.uploaded_file = UploadedFile(self.filename)

    def items(self):
        for k in list(self.keys()):
            yield (k, self[k])

    def iteritems(self):
        return list(self.items())

if sys.version_info[0] < 3:
    _use_python2_hacks = True
    from types import StringType, UnicodeType
    _string_types = {StringType, UnicodeType}
else:
    _string_types = {bytes, str}

from contextlib import contextmanager
from functools import wraps

import traceback

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
        return self._env['PATH_INFO']

    @property
    def qs(self):
        return self._env['QUERY_STRING']

    @property
    def unparsed_uri(self):
        qs = self.qs
        return self.uri + ('' if not qs else '?' + qs)

    @property
    def remote_ip(self):
        return self._env['REMOTE_ADDR']

    @property
    def method(self):
        return self._env['REQUEST_METHOD']

    @property
    def cookies(self):
        return http.cookies.SimpleCookie(self._env['HTTP_COOKIE'])

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

    def get_header_value(self, header_name):
        return self._env[header_name]

    def contains_header(self, header_name):
        return header_name in self._env

    def get_cookie_value(self, key):
        return self._env.cookies[key].value

    def log(self, *args, **kw):
        try:
            print(args[0], file=self._env['wsgi.errors'])
            return True
        except (KeyError, IndexError):
            return False

    def get_form_data(self, large_file=False):
        form_data = ItemizedFieldStorage(self.input, environ=self._env)

        return form_data

BUFFSIZE = 262144

status_message = {
    Return.HTTP_OK:                    'OK',
    Return.HTTP_MOVED_PERMANENTLY:     'Moved Permanently',
    Return.HTTP_FOUND:                 'Found',
    Return.HTTP_SEE_OTHER:             'See Other',
    Return.HTTP_NOT_MODIFIED:          'Not Modified',
    Return.HTTP_NOT_FOUND:             'Not Found',
    Return.HTTP_FORBIDDEN:             'Access Forbidden for This Resource',
    Return.HTTP_METHOD_NOT_ALLOWED:    'The Method Used to Access This Resource Is Not Allowed',
    Return.HTTP_NOT_ACCEPTABLE:        'The Returned Content Is Not Acceptable for the Client',
    Return.HTTP_NOT_IMPLEMENTED:       'Method Not Implemented',
    Return.HTTP_SERVICE_UNAVAILABLE:   'The Service Is Currently Unavailable',
    Return.HTTP_BAD_REQUEST:           'The Server Received a Bad Request -Probably Malformed JSON',
    Return.HTTP_INTERNAL_SERVER_ERROR: 'The Server has Found an Error Condition'
}

template_for_status = {
    'text/html:' + str(Return.HTTP_NOT_FOUND):             'errors/not_found.html',
    'text/html:' + str(Return.HTTP_FORBIDDEN):             'errors/forbidden.html',
    'application/json:' + str(Return.HTTP_FORBIDDEN):      'errors/forbidden.json',
    'text/html:' + str(Return.HTTP_INTERNAL_SERVER_ERROR): 'errors/internal_error.html',
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
        assert not self._started_response, "Asked to start a response, but headers have been sent"
        return fn(self, *args, **kw)
    return wrapper

class BufferedFileObjectIterator(object):
    def __init__(self, fobj, chunksize=BUFFSIZE):
        self.fobj  = fobj
        self.chksz = chunksize

    def __iter__(self):
        sz = self.chksz
        while True:
            n = self.fobj.read(sz)
            if not n:
                break
            yield n

class StreamingObject(object):
    """
    Helper file-like object that implements a buffered output. Useful as a target for json.dump
    and other functions producing large outputs that need to be streamed.

    A :py:class:`StreamingObject` will buffer the data written to it up to a certain limit,
    dumping the buffer to a certain output when it reaches its limit.
    """
    def __init__(self, callback, buffer_size = 0):
        """
        ``buffer_size`` is the threshold that needs to be reached before dumping
        the contents of the buffer. Size 0 means no buffering.

        :py:class:`StreamingObject` is output agnostic. It is initialized with a ``callback``
        that will be invoked passing the buffer contents as a string. This callback is responsible
        for delivering the buffer to the output.
        """
        self._callback = callback
        self._maxbuffer = buffer_size
        self._reset_buffer()

    def write(self, data):
        self._buffer.append(data)
        self._buffered += len(data)
        if self._buffered > self._maxbuffer:
            self.flush()

    def _reset_buffer(self):
        self._buffer = []
        self._buffered = 0

    def flush(self):
        """
        Dump the buffer contents right away.
        """
        self._callback(''.join(self._buffer).encode('utf8'))
        self._reset_buffer()

    def close(self):
        """
        Does nothing, except calling :py:meth:`StreamingObject.flush`
        """
        self.flush()

class JsonStreamingObject(object):
    """
    Helper file-like object that implements an unbuffered output, streaming JSON objects
    as they're written.
    """
    def __init__(self, callback):
        self._callback = callback

    def write(self, data):
        self._callback(json.dumps(data) + '\n')

    def flush(self):
        pass

    def close(self):
        pass

class Response(adapter.Response):
    def __init__(self, session, wsgienv, start_response):
        super(Response, self).__init__(session)

        self._env = wsgienv
        self._sr  = start_response
        self._bytes_sent = 0
        self._cookies_to_send = http.cookies.SimpleCookie()
        self.make_empty()
        self._started_response = False
        self._filter = lambda x: x
        self._write_callback = None
        self._content_type = 'text/plain'

    def respond(self, filter = None):
        self.start_response()
        if filter is not None:
            self._filter = filter

        return iter(self)

    def __iter__(self):
        f = self._filter

        for element in self._content:
            if type(element) in _string_types:
                r = f(element)
                if isinstance(r, str):
                    yield r.encode('utf8')
                else:
                    yield r
                self._bytes_sent += len(r)
            else:
                for subelement in element:
                    r = f(subelement)
                    yield r
                    self._bytes_sent += len(r)

    @property
    def bytes_sent(self):
        return self._bytes_sent

    def start_response(self):
        if not self._started_response:
            self._started_response = True
            self._write_callback = self._sr(
                '{} {}'.format(self.status, status_message.get(self.status, 'Error')),
                   [('Content-Type', self._content_type)]
                 + self._headers[:]
                 + [('Set-Cookie', morsel.OutputString()) for morsel in list(self._cookies_to_send.values())]
            )
        return self

    def expire_cookie(self, name):
        self.set_cookie(name, expires=datetime.utcnow())
        return self

    def set_cookie(self, name, value='', **kw):
        """
        Encodes a cookie using the value and other arguments passed as keywords.
        The ``expires`` argument requires a ``datetime`` object with UTC/GMT offset.
        """

        # The minimal
        self._cookies_to_send[name] = value
        ck = self._cookies_to_send[name]
        for k, v in kw.items():
            if k == 'expires':
                ck['expires'] = v.strftime('%a, %d %b %Y %H:%M:%S GMT')
            else:
                ck[k] = v

    def set_content_type(self, content_type):
        self._content_type = content_type
        return self

    def set_header(self, name, value):
        self._headers.append((name, value))
        return self

    @only_if_not_started_response
    def append(self, string):
        self._content.append(string)
        return self

    @only_if_not_started_response
    def append_iterable(self, it):
        self._content.append(it)
        return self

    @only_if_not_started_response
    def append_json(self, obj, **kw):
        """
        Takes an object and appends to the contents a serialized representation of it,
        encoded in JSON format.
        """
        self._content.append(json.dumps(obj, **kw))
        return self

    @only_if_not_started_response
    def send_json(self, obj, **kw):
        """
        Stream a JSON object. Intended for large contents, to avoid keeping them in memory.
        This is not the intended way to work with WSGI, though, so use ``append_json`` for
        other uses.
        """
        assert (len(self._content) == 0)
        if 'json' not in self._content_type:
            self.set_content_type('application/json')
        self.start_response()

        json.dump(obj, StreamingObject(self._write_callback), **kw)

    @only_if_not_started_response
    @contextmanager
    def streamed_json(self, **kw):
        """
        Stream a collection of JSON objects. Intended for multiple individual responses to
        a query. This is not the intended way to work with WSGI... but not much we can do
        about it.
        """
        if 'json' not in self._content_type:
            self.set_content_type('application/json')

        try:
            self.start_response()
            sobj = JsonStreamingObject(self._write_callback)
            yield sobj
        except Exception as e:
            self._env.log(str(e))
            self._env.log(traceback.format_exc())
        finally:
            sobj.close()

    def sendfile(self, path):
        self.sendfile_obj(open(path, 'rb'))

    def sendfile_obj(self, fp):
        self.append_iterable(BufferedFileObjectIterator(fp))

    @only_if_not_started_response
    @contextmanager
    def tarfile(self, name, **kw):
        assert (len(self._content) == 0)
        self.set_header('Content-Disposition', 'attachment; filename="{}"'.format(name))
        self.start_response()

        sobj = StreamingObject(self._write_callback)
        tar = tarfile.open(name=name, fileobj=sobj, **kw)

        try:
            yield tar
        finally:
            sobj.close()
            tar.close()

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
    def client_error(self, code, message=None, template=None, content_type='text/html', annotate=None):
        """
        Helper to raise 4xx "Client Error" exceptions
        """
        self.make_empty()
        self.set_content_type(content_type)
        self.status = code
        msg  = message if message is not None else status_message.get(code, 'Error: {}'.format(code))
        try:
            if template is None:
                template = template_for_status['{}:{}'.format(content_type, code)]
        except KeyError:
            self.append(msg)
        else:
            self.append(templating.get_env().get_template(template).render(message = msg))

        raise ClientError(code, msg, annotate)

from ...orm.usagelog import UsageLog

class ContextResponseIterator(object):
    def __init__(self, response, context_closer):
        self._resp = response
        self._cls  = context_closer
        self._ctx  = adapter.get_context()
        self._closed = False

    def __iter__(self):
        session = self._ctx.session
        try:
            for chunk in self._ctx.resp:
                yield chunk
        except Exception as e:
            self._ctx.usagelog.add_note(traceback.format_exc())
            self.close()
            raise

    def close(self):
        if not self._closed:
            ctx = self._ctx
            try:
                session = ctx.session
                session.commit()
                ctx.usagelog.set_finals(ctx)
                session.commit()
                session.close()
            finally:
                self._cls()
                self._closed = True

class ArchiveContextMiddleware(object):
    def __init__(self, app):
        self.ctx = None
        self.application = app
        self.bytes_sent = 0

    def __call__(self, environ, start_response):
        self.ctx = adapter.get_context(initialize=True)
        ctx = self.ctx
        # Setup the session and basics of the context
        try:
            session = sessionfactory()
            ctx.session = session

            request = Request(session, environ)
            response = Response(session, environ, start_response)
            ctx.setContent(request, response)
        except Exception as e:
            self.close()
            raise


        # Basics of the context are done, let's continue by creating an entry in the
        # usagelog, and associate it to the context, too
        try:
            usagelog = UsageLog(ctx)
            ctx.usagelog = usagelog

            try:
                ctx.usagelog.user_id = request.user.id
            except AttributeError:
                # No user defined
                pass
            session.add(usagelog)
            session.commit()
        except Exception as e:
            try:
                traceback.print_exc()
                session.commit()
                session.close()
                raise
            finally:
                self.close()

        try:
            result = self.application(environ, start_response)
            return ContextResponseIterator(result, self.close)
        except Exception as e:
            try:
                traceback.print_exc()
                session.commit()
                #  If response status is OK this means we got an error that hasn't been captured down the line
                usagelog.add_note(traceback.format_exc())
                if ctx.resp.status == Return.HTTP_OK:
                    ctx.resp.status = Return.HTTP_INTERNAL_SERVER_ERROR
                usagelog.set_finals(ctx)
                session.commit()
                session.close()
                raise
            finally:
                self.close()

    def close(self):
        adapter.invalidate_context()
