from thread import get_ident
from functools import wraps
from ...orm import NoResultFound
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

    def __getattr__(self, attr):
        try:
            return getattr(self.req, attr)
        except AttributeError:
            raise AttributeError("Unknown attribute '{}'".format(attr))

    def invalidate(self):
        self._valid = False
        del Context.__threads[get_ident()]

class Request(object):
    def __init__(self, session):
        self._s = session

    @property
    def session(self):
        return self._s

    @property
    def cookies(self):
        raise NotImplementedError("Request.cookie needs to be implemented by derived classes")

    @property
    def user(self):
        """
        Get the session cookie from the inner request object and find and return the
        user object, or None if it is not a valid session cookie
        """

        # Do we have a session cookie?
        try:
            cookie = self.cookies['gemini_archive_session']
        except KeyError:
            # No session cookie, not logged in
            return None

        # Find the user that we are
        try:
            return self._s.query(User).filter(User.cookie == cookie).one()
        except NoResultFound:
            # This is not a valid session cookie
            return None

class Response(object):
    pass
