"""
This module contains the WSGI application and middleware. The WSGI application
is 'application'...
"""
from sqlalchemy.exc import NoResultFound

from fits_storage.server.wsgi.middlewares import \
    ArchiveContextMiddleware, StaticServer
from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.response import RequestRedirect, ClientError
from fits_storage.server.wsgi.returnobj import Return

from fits_storage.server.wsgi.router import get_route, url_map, dispatch


def unicode_to_string(uni):
    return uni.encode('utf-8') if isinstance(uni, str) else uni


def core_handler(environ, start_response):
    ctx = get_context()
    req, resp = ctx.req, ctx.resp
    resp.set_header('Cache-Control', 'no-cache')
    resp.set_header('Expired', '-1')

    route = get_route(url_map)
    if route is None:
        resp.client_error(Return.HTTP_NOT_FOUND, "Could not find the requested resource")
    else:
        dispatch(*route)
        return resp.respond(unicode_to_string)


handle_with_static = StaticServer(core_handler)

def handler(environ, start_response):
    ctx = get_context()
    try:
        return handle_with_static(environ, start_response)
    except RequestRedirect as e:
        return ctx.resp.respond(unicode_to_string)
    except ClientError as e:
        if e.annotate is not None:
            session = ctx.session
            annotationClass = e.annotate
            try:
                log = session.query(annotationClass).\
                    filter(annotationClass.usagelog_id == ctx.usagelog.id).one()
            except NoResultFound:
                log = annotationClass(ctx.usagelog)
            if hasattr(e, 'message'):
                log.add_note(e.message)
            session.add(log)
        return ctx.resp.respond(unicode_to_string)

application = ArchiveContextMiddleware(handler)

# Provide a basic WSGI server for testing
if __name__ == '__main__':
    import wsgiref.simple_server

    server = 'localhost'
    port = 8000

    try:
        httpd = wsgiref.simple_server.make_server(server, port, application)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nExiting after Ctrl-c")