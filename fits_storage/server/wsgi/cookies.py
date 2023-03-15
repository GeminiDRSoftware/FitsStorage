class Cookies(object):
    """
    Dictionary-like object that handles querying and setting cookies. It is
    syntactic sugar that hides the real operation, likely access to the
    :any:`Request` and :any:`Response` objects.

       * ``cookies[KEY]``: returns the value of the corresponding key.
       * ``cookies[KEY] = VALUE``: sets a cookie (KEY, VALUE) pair that will
         be sent with the response. The rest of cookie attributes (expiration,
         etc.) will be the default ones.
       * ``del cookies[KEY]``: sets a cookie *expiration* message to be sent
         along with the response.
    """

    def __init__(self, req, resp):
        self._req = req
        self._resp = resp

    def __getitem__(self, key):
        return self._req.get_cookie_value(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self._resp.expire_cookie(key)

    def get(self, key, other=None):
        """
        To complete the dictionary-like behaviour, a ``get`` counterpart for
        the braces is provided. It won't rise a :py:exc:`KeyError` exception
        if the key doesn't exist. Instead, it will return `other`.
        """
        try:
            return self[key]
        except KeyError:
            return other

    def set(self, key, value, **kw):
        """
        Sets a cookie (``key``, ``value``) pair that will be sent along with
        the response. Other cookie attributes can be passed as keyword
        arguments.

        Refer to :py:class:`Cookie.Morsel` for a list of allowed attributes.
        """
        self._resp.set_cookie(key, value, **kw)
