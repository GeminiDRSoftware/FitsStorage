from functools import wraps
from contextlib import contextmanager
import http.cookies
import datetime
import json
import traceback
import tarfile
from html import escape

from fits_storage.web import templating

from fits_storage.server.wsgi.returnobj import Return
from fits_storage.server.wsgi.helperobjects import \
    StreamingObject, JsonArrayStreamingObject, BufferedFileObjectIterator


def only_if_not_started_response(fn):
    @wraps(fn)
    def wrapper(self, *args, **kw):
        assert not self._started_response, \
            "Asked to start a response, but headers have been sent"
        return fn(self, *args, **kw)
    return wrapper


class SetEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that understands sets.

    JSON doesn't have sets. We have these sometimes and want to be able to
    handle them. This encoder simply encodes them as lists.
    """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


# TODO - replace this with http.HTTPStatus.
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


class ClientError(Exception):
    """
    Raising this exception is the preferred way to generate error status
    response objects. The main benefit is that raising the exception will
    halt any other processing (eg. generation of templated content)

    The mandatory arguments are ``code``, which should be one of
    :any:`Return`'s members; and ``message``, which is a textual message
    to be incorporated in the response.

    Note that the ``ClientError`` exception is **not** be raised directly
    by the user. Instead, use :any:`Response.client_error`, which offers
    better control of the response.

    The ``ClientError`` exception should be used only lower level code
    (like the handler) to capture error conditions and handle them.
    """
    def __init__(self, code, message, annotate=None):
        self.code = code
        self.args = [message]
        self.annotate = annotate


class RequestRedirect(Exception):
    """
    Raising this exception will stop the processing (eg. generation of
    templated code) and force a redirect status code to be issued.

    Note that the ``RequestRedirect`` exception is not meant to be
    rised directly by user code, and should be used only by lower
    level code to capture and handle the redirection.

    To perform redirections, use instead :any:`Response.redirect_to`.
    """
    pass


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


class Response(object):

    def __init__(self, session, wsgienv, start_response):
        self._s = session
        self.status = Return.HTTP_OK
        self._env = wsgienv
        self._sr = start_response
        self._bytes_sent = 0
        self._cookies_to_send = http.cookies.SimpleCookie()
        self.make_empty()
        self._started_response = False
        self._filter = lambda x: x
        self._write_callback = None
        self._content_type = 'text/plain'

    def __iter__(self):
        f = self._filter

        for element in self._content:
            if type(element) in {bytes, str}:
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

    def respond(self, filter=None):
        self.start_response()
        if filter is not None:
            self._filter = filter

        return iter(self)

    def start_response(self):
        if not self._started_response:
            self._started_response = True
            self._write_callback = self._sr(
                f'{self.status} {status_message.get(self.status, "Error")}',
                [('Content-Type', self._content_type)] + self._headers[:]
                + [('Set-Cookie', morsel.OutputString()) for morsel in
                   list(self._cookies_to_send.values())]
            )
        return self

    def expire_cookie(self, name):
        """
        Adds the header needed to expire the clinet cookie named ``name``.
        """
        self.set_cookie(name, expires=datetime.datetime.utcnow())
        return self

    def set_cookie(self, name, value='', **kw):
        """
        Will add a client cookie. Attributes different to the name and value
        can be passed as keyword arguments.

        Refer to :py:class:`Cookie.Morsel` for a list of allowed attributes.

        Encodes a cookie using the value and other arguments passed as
        keywords. The ``expires`` argument requires a ``datetime`` object
        with UTC/GMT offset.
        """

        # The minimal
        self._cookies_to_send[name] = value
        ck = self._cookies_to_send[name]
        for k, v in kw.items():
            if k == 'expires':
                ck['expires'] = v.strftime('%a, %d %b %Y %H:%M:%S GMT')
            else:
                ck[k] = v

    @property
    def content_type(self):
        return self._content_type

    @content_type.setter
    def content_type(self, content_type):
        self._content_type = content_type

    @property
    def content_length(self):
        for header in self._headers:
            if 'Content-Length' in header:
                return header['Content-Length']

    @content_length.setter
    def content_length(self, content_length):
        self.set_header('Content-Length', str(content_length))

    def set_header(self, name, value):
        """
        Adds a header to be sent with the response.
        """

        self._headers.append((name, value))
        return self

    @only_if_not_started_response
    def append(self, string):
        """
        Appends content to be sent with the response, typically in the form
        of a string.
        """
        self._content.append(string)
        return self

    @only_if_not_started_response
    def append_iterable(self, it):
        """
        Appends content to be sent with the response. This function takes an
        iterable (any kind) which must yield strings.
        """
        self._content.append(it)
        return self

    @only_if_not_started_response
    def append_json(self, obj, **kw):
        """
        Takes an object and appends to the contents a serialized
        representation of it, encoded in JSON format. Any additional keyword
        arguments will be passed verbatim to :py:meth:`json.dumps`.

        Sets the content-type to application/json
        """
        if 'json' not in self._content_type:
            self.content_type = 'application/json'
        self._content.append(json.dumps(obj, cls=SetEncoder, **kw))
        return self

    @only_if_not_started_response
    def send_json(self, obj, **kw):
        """
        Stream a JSON object. Intended for large contents, to avoid keeping
        them in memory. Use :py:meth:`Response.append_json` when possible,
        though, as some frameworks don't like starting the responses early.
        This is not the intended way to work with WSGI, though, so use
        ``append_json`` for other uses.

        Calling :py:meth:`Response.send_json` will automatically set the
        Content-Type to ``application/json``.
        """
        assert (len(self._content) == 0)
        if 'json' not in self._content_type:
            self.content_type = 'application/json'
        self.start_response()

        json.dump(obj, StreamingObject(self._write_callback), **kw)



    def sendfile(self, path):
        """
        Takes the path to an existing file and adds its contents to the
        response payload.
        """
        # TODO - use with so that it closes the darn thing.
        self.sendfile_obj(open(path, 'rb'))

    def sendfile_obj(self, fp):
        """
        Takes a file-like object and adds its contents to the response payload.
        """
        self.append_iterable(BufferedFileObjectIterator(fp))

    @only_if_not_started_response
    @contextmanager
    def tarfile(self, name, **kw):
        """
        Context manager designed to stream tar files generated on the fly.
        Apart from a ``name`` for the file, it accepts additional keyword
        arguments, corresponding to the ones accepted by
        :py:func:`tarfile.open`. This manager yields a
        :py:class:`tarfile.Tarfile` object, intended to be used like::

          with resp.tarfile('filename.tar') as tar:
             tar.addfile( ... )
        """
        assert (len(self._content) == 0)
        self.set_header('Content-Disposition',
                        'attachment; filename="{}"'.format(name))
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
        """
        Stops the processing and returns an HTTP Redirect status code to the
        client, pointing to the specified ``url``.

        The default status code is 302 (HTTP Found). If you need a different
        status, pass one of the :any:`Return` members as the ``code`` keyword
        argument, eg.::

           resp.redirecto_to('http://my.url', code=Return.HTTP_SEE_OTHER)

        :py:meth:`Response.redirect_to` will raise a :any:`RequestRedirect`
        exception.
        """
        self.make_empty()
        self.content_type = 'text/html'
        # Set the status to 'code' if passed as an argument, else use 302
        # FOUND as default
        self.status = (kw['code'] if 'code' in kw else Return.HTTP_FOUND)

        display_location = escape(url)
        self.append(redirect_template.format(location=escape(url),
                                             display_location=display_location))
        self.set_header('Location', url)

        raise RequestRedirect()

    @only_if_not_started_response
    def client_error(self, code, message=None, template=None,
                     content_type='text/html', annotate=None):
        """
        Stops the processings and returns an HTTP error status code to the
        client. ``code`` must be a member of :any:`Return`.

        The simples way to invoke :py:meth:`Response.client_error` is to pass
        a code and nothing else. Default message and template will be
        assigned, according to the status code. The response can be
        controlled to a higher degree, though, by using the keyword
        arguments. ``content_type`` is self-explanatory. About the other ones:

            * ``message``
                A string of text that will be embedded in the error
                template. Used to send a more informative error message.
                This message will be added to the database log notes, too.
            * ``template``
                The *path* to a specific template to be used when generating
                the error response. As with the rest of the application, the
                path to the template must be relative to the "template root"
                directory.
            * ``annotate``
                ORM class to be used when annotating the message to the
                database log. If it is ``None``, the handler will decide on a
                sensible one (probably :py:class:`orm.UsageLog`).
        """
        self.make_empty()
        self.content_type = content_type
        self.status = code
        msg = message if message is not None else \
            status_message.get(code, 'Error: {}'.format(code))
        try:
            if template is None:
                template = template_for_status[f'{content_type}:{code}']
        except KeyError:
            self.append(msg)
        else:
            self.append(templating.get_env().get_template(template).
                        render(message=msg))

        raise ClientError(code, msg, annotate)
