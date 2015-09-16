import urllib2
import json
from functools import partial

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
        non_empty = filter(bool, (resource, self.pref, action) + args)
        path = '/'.join(non_empty)
        try:
            response = json.loads(urllib2.urlopen(path, json.dumps(kw)).read())
            if 'error' in response:
                raise ApiProxyError(response['error'])
            elif 'result' not in response:
                raise ApiProxyError("Invalid response: lacking 'result'")
            return response['result']
        except TypeError:
            raise ApiProxyError("The response message is not valid: {!r}".format(response))
        except urllib2.HTTPError as e:
            raise ApiProxyError("HTTP error when connecting to {}".format(path))
