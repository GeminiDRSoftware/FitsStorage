import json

from .environment import Environment
from .helperobjects import ItemizedFieldStorage


class Request(object):
    """
    Object encapsulating information related to the HTTP request and values
    derived from it. Apart from the documented methods, it presents a partial
    dictionary-like interface, as syntactic sugar to access the HTTP headers:

      * ``request[KEY]``: returns the value for the requested HTTP header
      * ``KEY in request``: ``True`` if a certain HTTP header is present in
        the query; ``False`` otherwise
    """

    def __init__(self, session, wsgienv):
        self._s = session
        self._env = Environment(wsgienv)
        self._fields = None

    def __getitem__(self, key):
        """
        Provides a dictionary-like interface for the request object to get
        headers
        """
        self.get_header_value(key)

    def __contains__(self, key):
        """
        Provides an interface for the request object to enable the query for
        headers using 'in'
        """
        return self.contains_header(key)

    def get_header_value(self, header_name):
        """
        Returns the value for the ``header_name`` HTTP header. Raises
        :py:exc:`KeyError` if the header doesn't exist.
        """
        return self._env[header_name]

    def contains_header(self, header_name):
        return header_name in self._env

    def get_cookie_value(self, key):
        return self._env.cookies[key].value

    @property
    def session(self):
        """
        Returns the current ORM ``session`` object.
        """
        return self._s

    @property
    def is_ajax(self):
        """
        ``True`` if the request is part of an AJAX call, ``False`` otherwise
        """
        try:
            return self['X-Requested-With'] == 'XmlHttpRequest'
        except KeyError:
            return False

    def get_form_data(self, large_file=False):
        """
        Returns an object with the same interface as
        :py:class:`cgi.FieldStorage`, with the contents of a form sent by a
        POST request.

        If we expect a large file to be sent, ``large_file`` should be set to
        True. Some implementations of ``FieldStorage`` may benefit from
        knowing this.
        """

        form_data = ItemizedFieldStorage(self.input, environ=self._env)

        return form_data

    @property
    def input(self):
        """
        A file-like object that can be used to read the raw contents of the
        request payload.
        """
        return self._env['wsgi.input']

    @property
    def env(self):
        """
        Dictionary-like object that let's access to environment variables.
        Useful for low-level access to information like hostname, remote IP,
        etc.
        """
        return self._env

    @property
    def raw_data(self):
        """
        Reads the whole request payload and returns it as-is, as a single
        string.
        """
        # TODO: get rid of this. Why not just call input.read() ???
        length = int(self._env['CONTENT_LENGTH']) \
            if self._env['CONTENT_LENGTH'] else 0
        return self.input.read(length) if length > 0 else ''

    @property
    def json(self):
        """
        Tries to interpret the request payload as a JSON encoded string,
        and returns the resulting object. It may raise a :py:exc:`ValueError`
        exception, if the payload is not valid JSON.
        """
        return json.loads(self.raw_data)

    def log(self, *args, **kw):
        """
        Log a message to the error output of the web server. The exact
        positional and keyword arguments depend on the implementation, but it
        is safe to assume that the first argument is the message to be printed.
        """
        try:
            print(args[0], file=self._env['wsgi.errors'])
            return True
        except (KeyError, IndexError):
            return False
