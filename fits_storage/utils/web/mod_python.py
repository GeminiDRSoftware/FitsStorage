import json

class Request(object):
    def __init__(self, req):
        self._req = req

    @property
    def raw_data(self):
        return req.read()

    @property
    def json(self):
        return json.loads(self.raw_data)
