import json

class Environment(object):
    def __init__(self, req):
        self._req = req

    @property
    def server_hostname(self):
        return self._req.server.server_hostname

class Request(object):
    def __init__(self, req):
        self._req = req
        self._env = Environment(req)
        self._log = Logger(req)

    @property
    def env(self):
        return self._env

    @property
    def log(self, *args, **kw):
        return self._req.log_error(*args, **kw)

    @property
    def raw_data(self):
        return self._req.read()

    @property
    def json(self):
        return json.loads(self.raw_data)
