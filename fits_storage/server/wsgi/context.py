"""
The context object is a useful concept carried over from the old mod_python
days. It contains various useful items representing the context of the web
request we are processing.
"""

from threading import local
from sqlalchemy.exc import NoResultFound

from .cookies import Cookies

from fits_storage.server.orm.user import User

from fits_storage.config import get_config
fsc = get_config()


# The context storage is a threading local-variable container. Each thread
# will see a different value
__ContextStorage__ = local()


def get_context(initialize=False):
    """
    This is a factory function that will return an initialized :any:`Context`
    object. Using this factory ensures that each thread in a multi-thread
    environment gets its own Context object, to avoid overlap.

    Calling ``get_context()`` within a thread of execution will return always
    the same object.

    The user shouldn't invoke ``get_context(initialize=True)``, which is
    meant only to create a fresh ``Context`` at the beginning of processing a
    new query.
    """

    if initialize:
        __ContextStorage__.ctx = Context()

    return __ContextStorage__.ctx


def invalidate_context():
    __ContextStorage__.ctx = None


class Context(object):
    """
    Main interface class. It exposes the request and response objects,
    the cookies, and any other environment variables.

    In addition, as shorthand, if one tries to access to an attribute that
    doesn't exist in ``Context``, it will return the equivalent attribute in
    the :any:`Request` object.

    Eg: ``ctx.session`` is the same as ``ctx.req.session``
    """
    def __init__(self):
        self.req = None
        self.resp = None
        self._cookies = None
        self.session = None

    def set_content(self, req, resp, session):
        """
        Sets the request/response objects, and initializes the cookies. Not
        meant to be used by user applications.
        """
        self.req = req
        self.resp = resp
        req.ctx = self
        resp.ctx = self
        self._cookies = Cookies(req, resp)
        self.session = session

    def __getattr__(self, attr):
        # TODO: Eventually, catch the AttributeErrors and raise them separately
        return getattr(self.req, attr)

    @property
    def cookies(self):
        """
        A :any:`Cookies` object, which exposes the cookies sent by the client
        along with the query, and allows to set new cookies to be sent with
        the response.
        """
        return self._cookies

    @property
    def got_magic(self):
        """
        It will be ``True`` if the client has the magic FITS authorization
        cookie, ``False`` otherwise.
        """

        if fsc.magic_download_cookie is None:
            return False

        try:
            return self.cookies['gemini_fits_authorization'] == \
                fsc.magic_download_cookie
        except KeyError:
            return False

    @property
    def user(self):
        """
        Returns the :any:`User` object corresponding to the current logged
        user, taken from the session cookie information; or ``None`` if the
        user is not logged in.
        """

        # Do we have a session cookie?
        try:
            cookie = self.ctx.cookies['gemini_archive_session']
        except KeyError:
            # No session cookie, not logged in
            return None

        # Find the user that we are
        try:
            return self.session.query(User).filter(User.cookie == cookie).one()
        except NoResultFound:
            # This is not a valid session cookie
            return None

    @property
    def is_staff(self):
        """
        ``True`` if the current logged-in user is a staff member, ``False``
        otherwise
        """
        try:
            return self.user.is_staff
        except AttributeError:
            return False
