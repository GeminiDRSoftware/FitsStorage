"""
This module contains the various middleware layers in our WSGI stack.

ArchiveContextMiddleware

StaticServer serves static files (such as CSS files, help pages etc.). We
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
if fsc.is_archive:
    from fits_storage.server.prefix_helpers import get_ipprefix_from_db


blocked_msg = """
Login Required. Please visit https://archive.gemini.edu/login

Unfortunately, and like many on line data repositories, the Gemini Observatory
Archive has been subject to a massive increase in excessive and/or malicious
requests, often associated with AI training robots and associated malware. 
Your IP address range or ISP has been the source of excessive or malicious 
requests to this server and anonymous access has been denied. If you are a 
genuine Gemini Observatory Archive user, we apologize for the inconvenience. 

To get access to the archive, simply log in at https://archive.gemini.edu/login.
If you do not already have an account, we recommend using the login via ORCID 
option. If you do not already have an ORCID ID, register for free at 
https://orcid.org/register
"""


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
    Takes care of:
    - providing the 'context', containing useful things like a database session,
    - looking up user privileges based on any session cookie in the request
    - adding an entry to the usagelog for each request that comes in.
    - denying anonymous requests from "blocked" IPPrefixes for the archive
    """

    def __init__(self, application):
        self.ctx = None
        self.application = application
        self.bytes_sent = 0
        fsc = get_config()
        self.is_archive = fsc.is_archive

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
                self.ctx.usagelog.user_id = self.ctx.user.id
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

        # If we're the archive, and not logged in, and not an allowed user agent prefix, check if we should block
        # this request. Note, the allow_user_agent_strings are *prefixes* to allow trailing version numbers.
        if self.is_archive and not self.ctx.usagelog.user_id and \
                True not in [self.ctx.req.env.user_agent.startswith(a) for a in fsc.allow_user_agent_strings]:
            # User agent check
            for badword in fsc.block_user_agent_substrings:
                if self.ctx.req.env.user_agent and \
                        badword in self.ctx.req.env.user_agent:
                    # Blocked!
                    usagelog.add_note(f"Blocked - User agent {badword}")
                    self.ctx.resp.content_type = 'text/plain'
                    self.ctx.resp.status = Return.HTTP_FORBIDDEN
                    usagelog.status = self.ctx.resp.status
                    session.commit()
                    return self.ctx.resp.append(blocked_msg).respond()
            # IPPrefix check - Find if this request comes from a known IPPrefix
            ipp = get_ipprefix_from_db(session, self.ctx.req.env.remote_ip)

            try:
                allowed_url = (
                    self.ctx.req.env.unparsed_uri.startswith('/login') or
                    self.ctx.req.env.unparsed_uri.startswith('/orcid') or
                    self.ctx.req.env.unparsed_uri.startswith('/noirlabsso') or
                    self.ctx.req.env.unparsed_uri.startswith('/logout'))
                # Maybe we should allow request_account etc. here too, but
                # that seems risky, so they'll need to use another ISP for that,
                # or just use orcid.
            except AttributeError:
                allowed_url = False

            if ipp and ipp.deny and not allowed_url:
                # Blocked!
                usagelog.add_note(f"Blocked - IPPrefix {ipp.prefix}")
                self.ctx.resp.content_type = 'text/plain'
                self.ctx.resp.status = Return.HTTP_FORBIDDEN
                usagelog.status = self.ctx.resp.status
                session.commit()
                return self.ctx.resp.append(blocked_msg).respond()

        # If we got here, we did not block the request
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
    Middleware class. An instance of StaticServer will intercept /static
    queries and return the static file (relative to certain root directory).

    Ideally, /static will be dealt with at a higher level. In that case,
    this doesn't introduce a significant overhead.
    """
    def __init__(self, application):
        fsc = get_config()
        self.app = application
        self.root = fsc.htmldocroot

    def __call__(self, environ, start_response):
        fsc = get_config()
        ctx = get_context()

        uri = list(filter(len, ctx.req.env.uri.split('/')))
        if len(uri) > 1 and uri[0] == 'static':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(fsc.htmldocroot, '/'.join(uri[1:]))
                if mtype is not None:
                    ctx.resp.content_type = mtype
                return ctx.resp.append(open(path, 'rb').read()).respond()
            except IOError:
                ctx.resp.client_error(Return.HTTP_FORBIDDEN)
        if len(uri) > 1 and uri[0] == 'help':
            mtype, enc = mimetypes.guess_type(uri[-1])
            try:
                path = os.path.join(fsc.htmldocroot, '/'.join(uri))
                if mtype is not None:
                    ctx.resp.content_type = mtype
                return ctx.resp.append(open(path, 'rb').read()).respond()
            except IOError:
                ctx.resp.client_error(Return.HTTP_FORBIDDEN)
        return self.app(environ, start_response)
