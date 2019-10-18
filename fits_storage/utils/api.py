import urllib.request, urllib.error, urllib.parse
import json
from functools import partial, wraps
import inspect
from importlib import import_module

import traceback
import sys

from .null_logger import EmptyLogger

##########################################################################################
#
# Frontend bits
#

class ApiProxyError(Exception):
    pass

class ApiProxy(object):
    def __init__(self, server, prefix=''):
        if ':' in server:
            server, port = server.split(':', 1)
        else:
            port = '80'

        self.server = server
        self.port   = int(port)
        self.pref   = prefix

    def __getattr__(self, attribute):
        return partial(self.__invoke, attribute)

    def __invoke(self, action, method='POST', *args, **kw):
        resource = 'http://{}:{}'.format(self.server, self.port)
        non_empty = list(filter(bool, (resource, self.pref, action) + args))
        path = '/'.join(non_empty)
        try:
            response = json.loads(urllib.request.urlopen(path, json.dumps(kw)).read())
            if 'error' in response:
                if 'error_object' in response:
                    eobj = response['error_object']
                    cls = getattr(import_module(eobj['module']), eobj['class'])
                    raise cls(eobj['content'])
                else:
                    raise ApiProxyError(response['error'])
            elif 'result' not in response:
                raise ApiProxyError("Invalid response: lacking 'result'")
            return response['result']
        except TypeError:
            raise ApiProxyError("The response message is not valid: {!r}".format(response))
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            raise ApiProxyError("HTTP error when connecting to {}".format(path))

##########################################################################################
#
# Backend bits
#

HTTP_OK            = 200
FORBIDDEN          = 403
NOT_FOUND          = 404
METHOD_NOT_ALLOWED = 405
INTERNAL_ERROR     = 500

status_text = {
    HTTP_OK           : "OK",
    FORBIDDEN         : "Forbidden",
    NOT_FOUND         : "Not Found",
    METHOD_NOT_ALLOWED: "Method not Allowed",
    INTERNAL_ERROR    : "Internal Error",
    }

def get_status_text(status):
    return "{} {}".format(status, status_text[status])

class NewCardsIncluded(Exception):
    pass

class WSGIError(Exception):
    def __init__(self, message, status=HTTP_OK, content_type = 'application/json', error_object = None):
        self.status  = status
        self.ct      = content_type
        self.message = message
        self.eobj    = error_object

    def response(self, environ, start_response):
        start_response(get_status_text(self.status), [('Content-Type', self.ct)])
        message = {'error': self.message}
        if self.eobj:
            cls = self.eobj.__class__
            message['error_object'] = {
                'module':  cls.__module__,
                'class':   cls.__name__,
                'content': self.eobj.message
            }
        return [json.dumps(message)]

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

class ApiCall(object):
    def __init__(self, call, logger):
        self._call  = call
        self.__doc__ = call.__doc__
        self.log     = logger

    def __call__(self, environ, start_response):
        query = get_arguments(environ)
        try:
            self.log.info("Calling %s with arguments %s", self._call.__name__, query)
            ret = self._call(**query)
        except TypeError as e:
            args, _, _, defaults = inspect.getargspec(self._call)
            non_default = set(args if defaults is None else args[:-len(defaults)])
            passed      = set(query)
            missing = non_default - passed
            extra   = passed - non_default
            if missing:
                raise WSGIError("Missing argument(s): {}".format(', '. join(missing)))
            elif extra:
                raise WSGIError("Unexpected argument(s): {}".format(', '. join(extra)))
            raise WSGIError(str(e))
        except Exception as e:
            string = ''.join(traceback.format_tb(sys.exc_info()[2])) + '\n\n' + str(e)
            raise WSGIError(string)

        try:
            result = json.dumps({'result': ret})
        except TypeError:
            raise WSGIError("Error when trying to prepare the result to be returned",
                            status=INTERNAL_ERROR)

        start_response("200 OK", [('Content-Type', 'application/json')])
        return [result]

def json_api_call(logger=None):
    def wrapper(fn):
        return wraps(fn)(ApiCall(fn, logger or EmptyLogger()))
    return wrapper
