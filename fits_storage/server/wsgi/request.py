import json
import multipart

from .environment import Environment


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
        self._wsgienv = wsgienv
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

    def get_form_data(self, getfile=None):
        """
        Parses form data, if getfile is not None, add the item with key=getfile
        from the files MultiDict into the forms dict
        """

        if multipart.is_form_request(self._wsgienv):
            forms, files = multipart.parse_form_data(self._wsgienv)
            if getfile:
                forms[getfile] = files.get(getfile)

            # Remove empty items from the dict.
            good = {}
            for (key, value) in forms.iterallitems():
                if value is None or value == '':
                    pass
                else:
                    if key not in good:
                        good[key] = value
                    else:
                        if not isinstance(good[key], list):
                            good[key] = [good[key]]
                        good[key].append(value)
            return good
        else:
            return {}

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

    def drain_input(self, already_read=0, max_drain=1 << 20,
                    chunksize=1 << 20):
        """
        Reads and discards any unread request payload. Call this before
        rejecting a request (eg with a POST upload) without reading its body,
        otherwise the server closes the connection with unread data pending,
        the TCP stack sends a RST, and the client sees ConnectionResetError
        instead of the error response.

        ``already_read`` is the number of payload bytes the caller has
        already consumed.

        ``max_drain`` caps how many bytes we will read and discard (default
        1MB), so that rejecting a huge upload doesn't mean receiving all of
        it. If more than this is pending, the client may still see a
        connection reset. Pass ``None`` for no limit.

        If there is a Content-Length, we never read past it, as that can hang
        under wsgiref.simple_server. If there isn't (eg a chunked upload), we
        read until end of input, but only if the server sets
        wsgi.input_terminated to say that the input stream ends at the end of
        the request body. Otherwise, reading could block forever, so we
        don't drain at all.
        """
        try:
            length = self._env['CONTENT_LENGTH']
        except KeyError:
            length = None

        if length:
            try:
                remaining = int(length) - already_read
            except ValueError:
                return
            if max_drain is not None:
                remaining = min(remaining, max_drain)
        elif 'wsgi.input_terminated' in self._env and \
                self._env['wsgi.input_terminated']:
            # Unknown length, but safe to read to EOF
            remaining = max_drain  # None means read to EOF
        else:
            return

        try:
            while remaining is None or remaining > 0:
                size = chunksize if remaining is None \
                    else min(chunksize, remaining)
                data = self.input.read(size)
                if not data:
                    break
                if remaining is not None:
                    remaining -= len(data)
        except OSError:
            # Connection already broken - nothing more we can do
            pass

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
