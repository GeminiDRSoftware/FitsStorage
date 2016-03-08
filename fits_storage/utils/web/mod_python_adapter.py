from . import adapter
from ...fits_storage_config import upload_staging_path

from mod_python import Cookie
from mod_python import util
from mod_python.apache import REMOTE_NOLOOKUP

import json
import time
import tarfile
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

class Environment(object):
    def __init__(self, req):
        self._req = req

    @property
    def server_hostname(self):
        return self._req.server.server_hostname

    @property
    def remote_host(self):
        return self._req.get_remote_host()

    @property
    def uri(self):
        return self._req.uri

    @property
    def unparsed_uri(self):
        return self._req.unparsed_uri

    @property
    def remote_ip(self):
        return self._req.get_remote_host(REMOTE_NOLOOKUP)

    @property
    def method(self):
        return self._req.method

class LargeFile(object):
    def __init__(self, *args, **kw):
        self.uploaded_file = None

    def __call__(self, name, delete=False):
        if self.uploaded_file is None:
            fobj = NamedTemporaryFile(mode='w+b', suffix='.' + name, dir=upload_staging_path, delete=delete)
            self.uploaded_file = fobj
        else:
            fobj = self.uploaded_file
        return fobj

class Request(adapter.Request):
    def __init__(self, session, req):
        super(Request, self).__init__(session)

        self._req     = req
        self._env     = Environment(req)
        self._fields  = None

    @property
    def env(self):
        return self._env

    @property
    def raw_data(self):
        return self._req.read()

    @property
    def json(self):
        return json.loads(self.raw_data)

    def get_header_value(self, header_name):
        return self._req.headers_in[header_name]

    def contains_header(self, header_name):
        return header_name in self._req.headers_in

    def get_cookie_value(self, key):
        return Cookie.get_cookies(self._req)[key].value

    def log(self, *args, **kw):
        return self._req.log_error(*args, **kw)

    def get_form_data(self, large_file=False):
        if large_file:
            form_data = util.FieldStorage(self._req, file_callback=LargeFile())
        else:
            form_data = util.FieldStorage(self._req)

        return form_data

BUFFSIZE = 262144

class Response(adapter.Response):
    def __init__(self, session, req):
        super(Response, self).__init__(session)

        self._req     = req

    @property
    def bytes_sent(self):
        return self._req.bytes_sent

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

    @contextmanager
    def tarfile(self, name, **kw):
        self.set_header('Content-Disposition', 'attachment; filename="{}"'.format(name))
        tar = tarfile.open(name=name, fileobj=self._req, **kw)

        try:
            yield tar
        finally:
            tar.close()
            self._req.flush()

    def redirect_to(self, url, **kw):
        util.redirect(self._req, url, **kw)
