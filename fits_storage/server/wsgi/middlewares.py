"""
This module contains the various middleware layers in our WSGI stack.

ArchiveContextMiddleware

StaticServer serves static files (such as CSS files, help pages etc). We
include this here so that the site is fully function if run through a purely
WSGI system such as the wsgiref.simple_server. In real-world deployments, the
web server would ideally be taking care of serving static pages directly
without passing them on to the WSGI layer.
"""
import os
import traceback
import mimetypes

from fits_storage.db import sessionfactory
from fits_storage.server.orm.usagelog import UsageLog

from .context import get_context, invalidate_context
from .returnobj import Return
from .request import Request
from .response import Response

from fits_storage.config import get_config
fsc = get_config()


class ContextResponseIterator(object):
    def __init__(self, response, context_closer):
        self._resp = response
        self._cls = context_closer
        self._ctx = get_context()
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
    """
    Takes care of providing the 'context' (containing useful things like a
    database session, and looking up user privileges based on any session
    cookie in the request) and also takes care of adding an entry to the
    usagelog for each request that comes in.
    """

    def __init__(self, application):
        self.ctx = None
        self.application = application
        self.bytes_sent = 0

    def __call__(self, environ, start_response):
        self.ctx = get_context(initialize=True)
        # Set up the session and basics of the context
        try:
            session = sessionfactory()

            request = Request(session, environ)
            response = Response(session, environ, start_response)
            self.ctx.set_content(request, response, session)
        except Exception as e:
            self.close()
            raise

        # Basics of the context are done, let's continue by creating an entry
        # in the usagelog, and associate it to the context, too
        try:
            usagelog = UsageLog(self.ctx)
            self.ctx.usagelog = usagelog

            try:
                self.ctx.usagelog.user_id = request.user.id
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
                # If response status is OK this means we got an error that
                # hasn't been captured down the line
                usagelog.add_note(traceback.format_exc())
                if self.ctx.resp.status == Return.HTTP_OK:
                    self.ctx.resp.status = Return.HTTP_INTERNAL_SERVER_ERROR
                usagelog.set_finals(self.ctx)
                session.commit()
                session.close()
                raise
            finally:
                self.close()

    def close(self):
        invalidate_context()


class StaticServer(object):
    """
    Middleware class. An instance of StaticServer will intercept /static queries and
    return the static file (relative to certain root directory).

    Ideally, /static will be dealt with at a higher level. In that case, this doesn't
    introduce a significative overhead.
    """
    def __init__(self, application):
        self.app  = application
        self.root = fsc.htmldocroot

    def __call__(self, environ, start_response):
        ctx = get_context()

        uri = list(filter(len, ctx.req.env.uri.split('/')))
        if len(uri) > 1 and uri[0] == 'static':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(fsc.htmldocroot, '/'.join(uri[1:]))
                if mtype is not None:
                    ctx.resp.set_content_type(mtype)
                return ctx.resp.append(open(path, 'rb').read()).respond()
            except IOError:
                ctx.resp.client_error(Return.HTTP_FORBIDDEN)
        if len(uri) > 1 and uri[0] == 'help':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(fsc.htmldocroot, '/'.join(uri))
                if mtype is not None:
                    ctx.resp.set_content_type(mtype)
                return ctx.resp.append(open(path, 'rb').read()).respond()
            except IOError:
                ctx.resp.client_error(Return.HTTP_FORBIDDEN)
        return self.app(environ, start_response)
