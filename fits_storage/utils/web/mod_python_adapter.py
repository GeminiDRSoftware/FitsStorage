from . import adapter
from mod_python import Cookie
import json

class Environment(object):
    def __init__(self, req):
        self._req = req

    @property
    def server_hostname(self):
        return self._req.server.server_hostname

class Cookies(object):
    def __init__(self, req):
        self._req = req

    def __getitem__(self, key):
        return Cookie.get_cookies(self._req)[key].value

class Request(adapter.Request):
    def __init__(self, session, req):
        super(Request, self).__init__(session)

        self._req     = req
        self._env     = Environment(req)
        self._cookies = Cookies(req)

    @property
    def env(self):
        return self._env

    @property
    def cookies(self):
        return self._cookies

    def log(self, *args, **kw):
        return self._req.log_error(*args, **kw)

    @property
    def raw_data(self):
        return self._req.read()

    @property
    def json(self):
        return json.loads(self.raw_data)
