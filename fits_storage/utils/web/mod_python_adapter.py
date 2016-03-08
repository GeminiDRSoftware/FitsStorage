from . import adapter
from mod_python import Cookie
import json
import time

class Environment(object):
    def __init__(self, req):
        self._req = req

    @property
    def server_hostname(self):
        return self._req.server.server_hostname

    @property
    def uri(self):
        return self._req.uri

class Request(adapter.Request):
    def __init__(self, session, req):
        super(Request, self).__init__(session)

        self._req     = req
        self._env     = Environment(req)

    @property
    def env(self):
        return self._env

    @property
    def raw_data(self):
        return self._req.read()

    @property
    def json(self):
        return json.loads(self.raw_data)

    def get_cookie_value(self, key):
        return Cookie.get_cookies(self._req)[key].value

    def log(self, *args, **kw):
        return self._req.log_error(*args, **kw)

BUFFSIZE = 262144

class Response(adapter.Response):
    def __init__(self, session, req):
        super(Response, self).__init__(session)

        self._req     = req

    def expire_cookie(self, name):
        self.set_cookie(name, expires=time.time())

    def set_cookie(self, name, value='', **kw):
        Cookie.add_cookie(self._req, name, value, **kw)

    def set_content_type(self, content_type):
        self._req.content_type = content_type

    def set_header(self, name, value):
        self._req.headers_out[name] = value

    def append(self, string):
        self._req.write(string)

    def append_iterable(self, it):
        for text in it:
            self._req.write(text)

    def append_json(self, obj, **kw):
        json.dump(obj, self._req, **kw)

    def sendfile(self, path):
        self._req.sendfile(path)

    def sendfile_obj(self, fp):
        while True:
            n = fp.read(BUFFSIZE)
            if not n:
                break
            self._req.write(n)
