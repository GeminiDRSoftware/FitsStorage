from thread import get_ident
from functools import wraps
from ...orm import NoResultFound, MultipleResultsFound
from ...orm.user import User
from ...fits_storage_config import magic_download_cookie

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
    }

    def __getattr__(cls, key):
        try:
            return ReturnMetaClass.__return_codes[key]
        except KeyError:
            raise AttributeError("No return code {}".format(key))

class Return(object):
    __metaclass__ = ReturnMetaClass

class ClientError(Exception):
    def __init__(self, code):
        self.code = code

class RequestRedirect(Exception):
    pass

def context_wrapped(fn):
    @wraps(fn)
    def wrapper(*args, **kw):
        ctx = Context()
        try:
            return fn(ctx, *args, **kw)
        finally:
            ctx.invalidate()
    return wrapper

def with_content_type(content_type):
    def content_decorator(fn):
        @wraps(fn)
        def fn_wrapper(*args, **kw):
            Context().resp.set_content_type(content_type)
            return fn(*args, **kw)

        return fn_wrapper
    return content_decorator

class Context(object):
    __threads = {}
    def __new__(cls):
        this = get_ident()
        try:
            ret = Context.__threads[this]
            # This should never happen, but...
            if not ret._valid:
                ret = Context.__threads[this] = object.__new__(cls)
        except KeyError:
            ret = Context.__threads[this] = object.__new__(cls)

        return ret

    def __init__(self):
        self._valid  = True

    def setContent(self, req, resp):
        self.req = req
        self.resp = resp
        req.ctx = self
        resp.ctx = self
        self._cookies = Cookies(req, resp)

    def __getattr__(self, attr):
        # TODO: Eventually, catch the AttributeErrors and raise them separately
        return getattr(self.req, attr)

    def invalidate(self):
        self._valid = False
        del Context.__threads[get_ident()]

    @property
    def cookies(self):
        return self._cookies

    @property
    def got_magic(self):
        """
        Returns a boolean to say whether or not the client has
        the magic authorization cookie
        """

        if magic_download_cookie is None:
            return False

        try:
            return self.cookies['gemini_fits_authorization'] == magic_download_cookie
        except KeyError:
            return False

class Cookies(object):
    def __init__(self, req, resp):
        self._req  = req
        self._resp = resp

    def __getitem__(self, key):
        return self._req.get_cookie_value(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self._resp.expire_cookie(key)

    def set(self, key, value, **kw):
        self._resp.set_cookie(key, value, **kw)

class Request(object):
    def __init__(self, session):
        self._s = session

    def get_header_value(self, header_name):
        raise NotImplementedError("get_header_value must be implemented by derived classes")

    def __getitem__(self, key):
        "Provides a dictionary-like interface for the request object to get headers"
        self.get_header_value(key)

    def __contains__(self, key):
        "Provides a interface for the request object to enable the query for headers using 'in'"
        return self.contains_header(key)

    @property
    def session(self):
        return self._s

    @property
    def user(self):
        """
        Get the session cookie from the inner request object and find and return the
        user object, or None if it is not a valid session cookie
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
        try:
            return self.user.is_staffer
        except AttributeError:
            return False

    @property
    def is_ajax(self):
        "Returns a boolean to say if the request came in via ajax"
        try:
            return self['X-Requested-With'] == 'XmlHttpRequest'
        except KeyError:
            return False

    def get_form_data(self, large_file=False):
        raise NotImplementedError("get_form_data must be implemented by derived classes")

class Response(object):
    def __init__(self, session):
        self._s = session
        self.status = Return.HTTP_OK

    def expire_cookie(self, name):
        raise NotImplementedError("expire_cookie must be implemented by derived classes")

    def set_cookie(self, name, value='', **kw):
        raise NotImplementedError("set_cookie must be implemented by derived classes")

    def set_content_type(self, content_type):
        raise NotImplementedError("set_content_type must be implemented by derived classes")

    def content_type_setter(self, content_type):
        self.set_content_type(content_type)
    content_type = property(fset=content_type_setter)

    def content_length_setter(self, content_length):
        self.set_header('Content-Length', str(content_length))
    content_length = property(fset=content_length_setter)

    def set_header(self, name, value):
        raise NotImplementedError("set_header must be implemented by derived classes")

    def append(self, string):
        raise NotImplementedError("append must be implemented by derived classes")

    def append_iterable(self, it):
        raise NotImplementedError("append_iterable must be implemented by derived classes")

    def append_json(self, obj, **kw):
        raise NotImplementedError("append_json must be implemented by derived classes")

    def sendfile(self, path):
        raise NotImplementedError("sendfile must be implemented by derived classes")

    def sendfile_obj(self, fp):
        raise NotImplementedError("sendfile_obj must be implemented by derived classes")

    def tarfile(self, name, **kw):
        "This method will be used as a context manager. The implementation must ensure the proper interface"
        raise NotImplementedError("tarfile must be implemented by derived classes")

    def redirect_to(self, url, **kw):
        raise NotImplementedError("redirect_to must be implemented by derived classes")
