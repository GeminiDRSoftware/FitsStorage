#!/usr/bin/env python

"""There are certain operations where the web server would need to access
   and modify files. For this, we'd need the server to have permissions over
   the files, which we don't want, because of the potential security
   problems.

   Here we have instead a backend API server that isolates the user interface
   from the actual file operations"""

#######################################################################################
#
#   Generic stuff, mainly to control the routing and error responses
#

import json
import wsgiref.simple_server
from wsgiref.validate import validator
from wsgiref.util import request_uri, application_uri, shift_path_info
import pyfits as pf

from fits_storage.utils.fitseditor import compare_cards, modify_multiple_cards
from fits_storage.fits_storage_config import api_backend_location

HTTP_OK            = 200
FORBIDDEN          = 403
NOT_FOUND          = 404
METHOD_NOT_ALLOWED = 405

status_text = {
    HTTP_OK           : "OK",
    FORBIDDEN         : "Forbidden",
    NOT_FOUND         : "Not Found",
    METHOD_NOT_ALLOWED: "Method not Allowed",
    }

def get_status_text(status):
    return "{} {}".format(status, status_text[status])

class WSGIError(Exception):
    def __init__(self, message, status=HTTP_OK, content_type = 'application/json'):
        self.status  = status
        self.ct      = content_type
        self.message = message

    def response(self, environ, start_response):
        start_response(get_status_text(self.status), [('Content-Type', self.ct)])
        yield '{{"error": "{}"}}'.format(self.message)

def get_route(environ, routes):
    req_meth  = environ['REQUEST_METHOD']
    path_info = environ['PATH_INFO']
    try:
        by_method = routes[req_meth]
    except KeyError:
        raise WSGIError("'{}' is not a valid method for this server".format(req_meth), status=METHOD_NOT_ALLOWED)

    try:
        # TODO: This needs to be expanded to recognize variable paths, mainly for GET
        #       but also for PUT/DELETE
        return by_method[path_info]
    except KeyError:
        raise WSGIError("'{}' not a valid query for method '{}'".format(path_info, req_meth), status=NOT_FOUND)

def get_post_data(environ):
    try:
        return environ['wsgi.input'].read(int(environ.get('CONTENT_LENGTH', 0)))
    except ValueError:
        return ''

def get_arguments(environ):
    try:
        return json.loads(get_post_data(environ))
    except TypeError:
        raise WSGIError("The query is not a valid JSON method call")
    except ValueError:
        raise WSGIError("The data for this query is not valid JSON")

#######################################################################################
#
#   Helper functions
#

def fits_is_unchanged(path, new_values):
    return all(compare_cards(path, new_values, ext=0))

def fits_apply_changes(path, changes):
    if fits_is_unchanged(path, changes):
        return False

    modify_multiple_cards(path, changes, ext=0)
    return True

#######################################################################################
#
#   API Code
#

def set_image_metadata(environ, start_response):
    try:
        query = get_arguments(environ)
        path    = query['path']
        changes = query['changes']
    except KeyError as e:
        raise WSGIError("Missing argument '{}'".format(e.message))

    try:
        result = fits_apply_changes(path, changes)
    except (pf.VerifyError, IOError):
        raise WSGIError("There were problems when opening/modifying the file")

    start_response("200 OK", [('Content-Type', 'application/json')])
    return [json.dumps({'result': result})]

#######################################################################################
#
#   Routes and application entry point
#

routes = {
    'POST': {
        '/set_image_metadata': set_image_metadata,
    }
}

def app(environ, start_response):
    try:
        route = get_route(environ, routes)
        return route(environ, start_response)
    except WSGIError as e:
        return e.response(environ, start_response)

# Provide a basic WSGI server, in case we're testing or don't need any fancy
# container...
if __name__ == '__main__':
    try:
        server, port = api_backend_location.split(':')
    except ValueError:
        server = api_backend_location
        port   = '8000'

    httpd = wsgiref.simple_server.make_server(server, int(port), app)
    httpd.serve_forever()
