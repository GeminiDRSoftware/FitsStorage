from functools import wraps
from ...orm import NoResultFound, MultipleResultsFound
from ...orm.user import User
from ...fits_storage_config import magic_download_cookie
from _thread import get_ident
from threading import local
import abc
import json

class ReturnMetaClass(type):
    __return_codes = {
        'HTTP_OK': 200,
        'HTTP_MOVED_PERMANENTLY': 301,
        'HTTP_FOUND': 302,
        'HTTP_SEE_OTHER': 303,
        'HTTP_NOT_MODIFIED': 304,
        'HTTP_NOT_FOUND': 404,
        'HTTP_FORBIDDEN': 403,
        'HTTP_METHOD_NOT_ALLOWED': 405,
        'HTTP_NOT_ACCEPTABLE': 406,
        'HTTP_NOT_IMPLEMENTED': 501,
        'HTTP_SERVICE_UNAVAILABLE': 503,
        'HTTP_BAD_REQUEST': 400,
        'HTTP_INTERNAL_SERVER_ERROR': 500,
    }

    def __getattr__(cls, key):
        try:
            return ReturnMetaClass.__return_codes[key]
        except KeyError:
            raise AttributeError("No return code {}".format(key))

class Return(object, metaclass=ReturnMetaClass):
    """
    This is a specialized class with constant members giving names to
    HTTP Status Codes. These members are:

      * ``Return.HTTP_OK``
      * ``Return.HTTP_MOVED_PERMANENTLY``
      * ``Return.HTTP_FOUND``
      * ``Return.HTTP_SEE_OTHER``
      * ``Return.HTTP_NOT_MODIFIED``
      * ``Return.HTTP_NOT_FOUND``
      * ``Return.HTTP_FORBIDDEN``
      * ``Return.HTTP_METHOD_NOT_ALLOWED``
      * ``Return.HTTP_NOT_ACCEPTABLE``
      * ``Return.HTTP_NOT_IMPLEMENTED``
      * ``Return.HTTP_SERVICE_UNAVAILABLE``
      * ``Return.HTTP_BAD_REQUEST``
      * ``Return.HTTP_INTERNAL_SERVER_ERROR``
    """

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
        self.code     = code
        self.args     = [message]
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

def context_wrapped(fn):
    @wraps(fn)
    def wrapper(*args, **kw):
        ctx = get_context()
        try:
            return fn(ctx, *args, **kw)
        finally:
            invalidate_context()
    return wrapper

def with_content_type(content_type):
    def content_decorator(fn):
        @wraps(fn)
        def fn_wrapper(*args, **kw):
            get_context().resp.set_content_type(content_type)
            return fn(*args, **kw)

        return fn_wrapper
    return content_decorator

# The context storage is a threading local-variable container. Eache
# thread will see a different value
__ContextStorage__ = local()

def get_context(initialize = False):
    """
    This is a factory function that will return an initialized :any:`Context` object.
    Using this factory ensures that each thread in a multi-thread environment gets
    its own Context object, to avoid overlap.

    Calling ``get_context()`` within a thread of execution will return always the same
    object.

    The user shouldn't invoke ``get_context(initialize=True)``, which is meant only
    to create a fresh ``Context`` at the beginning of processing a new query.
    """
    if not initialize:
        ctx = __ContextStorage__.ctx
    else:
        ctx = Context()
        __ContextStorage__.ctx = ctx

    return ctx

def invalidate_context():
    __ContextStorage__.ctx = None

class Context(object):
    """
    Main interface class. It exposes the request and response objects, the cookies,
    and any other environment variables.

    In addition, as shorthand, if one tries to access to an attribute that doesn't
    exist in ``Context``, it will return the equivalent attribute in the :any:`Request`
    object.

    Eg: ``ctx.session`` is the same as ``ctx.req.session``
    """
    def __init__(self):
        self.req = None
        self.resp = None
        self._cookies = None

    def setContent(self, req, resp):
        """
        Sets the request/response objects, and initializes the cookies. Not meant to
        be used by user applications.
        """
        self.req = req
        self.resp = resp
        req.ctx = self
        resp.ctx = self
        self._cookies = Cookies(req, resp)

    def __getattr__(self, attr):
        # TODO: Eventually, catch the AttributeErrors and raise them separately
        return getattr(self.req, attr)

    @property
    def cookies(self):
        """
        A :any:`Cookies` object, which exposes the cookies sent by the client along
        with the query, and allows to set new cookies to be sent with the response.
        """
        return self._cookies

    @property
    def got_magic(self):
        """
        It will be ``True`` if the client has the magic FITS authorization cookie,
        ``False`` otherwise.
        """

        if magic_download_cookie is None:
            return False

        try:
            return self.cookies['gemini_fits_authorization'] == magic_download_cookie
        except KeyError:
            return False

class Cookies(object):
    """
    Dictionary-like object that handles querying and setting cookies. It is syntactic
    sugar that hides the real operation, likely acccess to the :any:`Request` and
    :any:`Response` objects.

       * ``cookies[KEY]``: returns the valor associated to the corresponding key,
       * ``cookies[KEY] = VALUE``: sets a cookie (KEY, VALUE) pair that will be sent
         with the response. The rest of cookie attributes (expiration, etc.) will be
         the default ones.
       * ``del cookies[KEY]``: sets a cookie *expiration* message to be sent along
         with the response.
    """
    def __init__(self, req, resp):
        self._req  = req
        self._resp = resp

    def __getitem__(self, key):
        return self._req.get_cookie_value(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self._resp.expire_cookie(key)

    def get(self, key, other=None):
        """
        To complete the dictionary-like behaviour, a ``get`` counterpart for the
        braces is provided. It won't rise a :py:exc:`KeyError` exception if the key
        doesn't exist. Instead, it will return `other`.
        """
        try:
            return self[key]
        except KeyError:
            return other

    def set(self, key, value, **kw):
        """
        Sets a cookie (``key``, ``value``) pair that will be sent along with the
        response. Other cookie attributes can be passed as keyword arguments.

        Refer to :py:class:`Cookie.Morsel` for a list of allowed attributes.
        """
        self._resp.set_cookie(key, value, **kw)

class Request(object, metaclass=abc.ABCMeta):
    """
    Object encapsulating information related to the HTTP request and values derived from it.
    Apart from the documented methods, it presents a partial dictionary-like interface, as
    syntactic sugar to access the HTTP headers:

      * ``request[KEY]``: returns the value for the requested HTTP header
      * ``KEY in request``: ``True`` if a certain HTTP header is present in the query; ``False`` otherwise
    """
    def __init__(self, session):
        self._s = session

    @abc.abstractmethod
    def get_header_value(self, header_name):
        """
        Returns the value for the the ``header_name`` HTTP header. Raises :py:exc:`KeyError` if the
        header doesn't exist.
        """

    def __getitem__(self, key):
        "Provides a dictionary-like interface for the request object to get headers"
        self.get_header_value(key)

    def __contains__(self, key):
        "Provides a interface for the request object to enable the query for headers using 'in'"
        return self.contains_header(key)

    @property
    def session(self):
        """
        Returns the current ORM ``session`` object.
        """
        return self._s

    @property
    def user(self):
        """
        Returns the :any:`User` object corresponding to the current logged user, taken
        from the session cookie information; or ``None`` if the user is not logged in.
        """

        # Do we have a session cookie?
        try:
            cookie = self.ctx.cookies['gemini_archive_session']
        except KeyError:
            # No session cookie, not logged in
            return None

        # Find the user that we are
        try:
            return self._s.query(User).filter(User.cookie == cookie).one()
        except NoResultFound:
            # This is not a valid session cookie
            return None
        except MultipleResultsFound:
            return self._s.query(User).filter(User.cookie == cookie).all()

    @property
    def is_staffer(self):
        """
        ``True`` if the current logged-in user is a staff member, ``False`` otherwise
        """
        try:
            return self.user.is_staffer
        except AttributeError:
            return False

    @property
    def is_ajax(self):
        """
        ``True`` if the request is part of an AJAX call, ``False`` otherwise
        """
        try:
            return self['X-Requested-With'] == 'XmlHttpRequest'
        except KeyError:
            return False

    @abc.abstractmethod
    def get_form_data(self, large_file=False):
        """
        Returns an object with the same interface as :py:class:`cgi.FieldStorage`, with the
        contents of a form sent by a POST request.

        If we expect a large file to be sent, ``large_file`` should be set to True. Some
        implementations of ``FieldStorage`` may benefit from knowing this.
        """

    @abc.abstractproperty
    def input(self):
        """
        A file-like object that can be used to read the raw contents of the request payload.
        """

    @abc.abstractproperty
    def env(self):
        """
        Dictionary-like object that let's access to environment variables. Useful for low-level
        access to information like hostname, remote IP, etc.
        """

    @abc.abstractproperty
    def raw_data(self):
        """
        Reads the whole request payload and returns it as-is, as a single string.
        """

    @property
    def json(self):
        """
        Tries to interpret the request payload as a JSON encoded string, and returns
        the resulting object. It may raise a :py:exc:`ValueError` exception, if the
        payload is not valid JSON.
        """
        return json.loads(self.raw_data)

    @abc.abstractmethod
    def log(self, *args, **kw):
        """
        Log a message to the error output of the web server. The exact positional and
        keyword argments depend on the implementation, but it is safe to assume that
        the first argument is the message to be printed.
        """

class Response(object):
    __metadata__ = abc.ABCMeta
    def __init__(self, session):
        self._s = session
        self.status = Return.HTTP_OK

    @abc.abstractmethod
    def expire_cookie(self, name):
        """
        Adds the header needed to expire the clinet cookie named ``name``.
        """

    @abc.abstractmethod
    def set_cookie(self, name, value='', **kw):
        """
        Will add a client cookie. Attributes different to the name and value can be passed
        as keyword arguments.

        Refer to :py:class:`Cookie.Morsel` for a list of allowed attributes.
        """

    @abc.abstractmethod
    def set_content_type(self, content_type):
        """
        Sets the content type for the response payload.
        """

    def content_type_setter(self, content_type):
        self.set_content_type(content_type)
    content_type = property(fset=content_type_setter,
                              doc='This is a property setter (write-only). It is intended to be used as a shortcut instead of :py:meth:`Response.set_content_type`')

    def content_length_setter(self, content_length):
        self.set_header('Content-Length', str(content_length))
    content_length = property(fset=content_length_setter,
                              doc='This is a property setter (write-only). It is intended to be used as a shortcut instead of :py:meth:`Response.set_header`')

    @abc.abstractmethod
    def set_header(self, name, value):
        """
        Adds a header to be sent with the response.
        """

    @abc.abstractmethod
    def append(self, string):
        """
        Appends content to be sent with the response, typically in the form of a string.
        """

    @abc.abstractmethod
    def append_iterable(self, it):
        """
        Appends content to be sent with the response. This function takes an iterable (any kind)
        which must yield strings.
        """

    @abc.abstractmethod
    def append_json(self, obj, **kw):
        """
        Takes an object and appends to the contents a serialized representation of it,
        encoded in JSON format. Any additional keyword arguments will be passed verbatim
        to :py:meth:`json.dumps`.
        """

    @abc.abstractmethod
    def send_json(self, obj, **kw):
        """
        Stream a JSON object. Intended for large contents, to avoid keeping them in memory.
        Use :py:meth:`Response.append_json` when possible, though, as some frameworks don't
        like starting the responses early.

        Calling :py:meth:`Response.send_json` will automatically set the Content-Type to
        ``application/json``.
        """

    @abc.abstractmethod
    def sendfile(self, path):
        """
        Takes the path to an existing file and adds its contents to the response payload.
        """

    @abc.abstractmethod
    def sendfile_obj(self, fp):
        """
        Takes a file-like object and adds its contents to the response payload.
        """

    @abc.abstractmethod
    def tarfile(self, name, **kw):
        """
        Context manager designed to stream tar files generated on the fly. Apart from a
        ``name`` for the file, it accepts additional keyword arguments, corresponding to
        the ones accepted by :py:func:`tarfile.open`. This manager yields a
        :py:class:`tarfile.Tarfile` object, intended to be used like::

          with resp.tarfile('filename.tar') as tar:
             tar.addfile( ... )
        """

    @abc.abstractmethod
    def redirect_to(self, url, **kw):
        """
        Stops the processing and returns an HTTP Redirect status code to the client, pointing
        to the specified ``url``.

        The default status code is 302 (HTTP Found). If you need a different status, pass
        one of the :any:`Return` members as the ``code`` keyword argument, eg.::

           resp.redirecto_to('http://my.url', code=Return.HTTP_SEE_OTHER)

        :py:meth:`Response.redirect_to` will raise a :any:`RequestRedirect` exception.
        """

    @abc.abstractmethod
    def client_error(self, code, message=None, template=None, content_type='text/html', annotate=None):
        """
        Stops the processings and returns an HTTP error status code to the client. ``code``
        must be a member of :any:`Return`.

        The simples way to invoke :py:meth:`Response.client_error` is to pass a code and nothing
        else. Default message and template will be assigned, according to the status code.
        The response can be controlled to a higher degree, though, by using the keyword arguments.
        ``content_type`` is self-explanatory. About the other ones:

            * ``message``
                  A string of text that will be embedded in the error template. Used to send
                  the user a more informative error message.

                  This message will be added to the database log notes, too.
            * ``template``
                  The *path* to a specific template to be used when generating the error
                  response. As with the rest of the application, the path to the template
                  must be relative to the "template root" directory.
            * ``annotate``
                  ORM class to be used when annotating the message to the database log. By
                  If it is ``None``, the handler will decide on a sensible one (probably
                  :py:class:`orm.UsageLog`).
        """
