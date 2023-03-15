import http.cookies


class Environment(object):
    def __init__(self, env):
        self._env = env

    def __getitem__(self, item):
        return self._env[item]

    def __contains__(self, item):
        return item in self._env

    @property
    def server_hostname(self):
        return self._env['SERVER_NAME']

    @property
    def remote_host(self):
        try:
            return self._env['HTTP_X_FORWARDED_FOR'].split(',')[-1].strip()
        except KeyError:
            return self.remote_ip

    @property
    def uri(self):
        return self._env['PATH_INFO']

    @property
    def qs(self):
        return self._env['QUERY_STRING']

    @property
    def unparsed_uri(self):
        qs = self.qs
        return self.uri + ('' if not qs else '?' + qs)

    @property
    def remote_ip(self):
        return self._env['REMOTE_ADDR']

    @property
    def method(self):
        return self._env['REQUEST_METHOD']

    @property
    def cookies(self):
        return http.cookies.SimpleCookie(self._env['HTTP_COOKIE'])
