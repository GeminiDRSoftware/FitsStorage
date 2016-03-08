from thread import get_ident
from functools import wraps
from ...orm import NoResultFound, MultipleResultsFound
from ...orm.user import User

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

class Response(object):
    def __init__(self, session):
        self._s = session

    def expire_cookie(self, name):
        raise NotImplementedError("expire_cookie must be implemented by derived classes")

    def set_cookie(self, name, value='', **kw):
        raise NotImplementedError("set_cookie must be implemented by derived classes")

    def set_content_type(self, content_type):
        raise NotImplementedError("set_content_type must be implemented by derived classes")

    def content_type_setter(self, content_type):
        self.set_content_type(content_type)
    content_type = property(fset=content_type_setter)

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
