# TODO: Is this really necessary??? Surely we can just use http.HTTPStatus ?

class ReturnMetaClass(type):
    __return_codes = {
        'HTTP_OK': 200,
        'HTTP_MOVED_PERMANENTLY': 301,
        'HTTP_FOUND': 302,
        'HTTP_SEE_OTHER': 303,
        'HTTP_NOT_MODIFIED': 304,
        'HTTP_NOT_FOUND': 404,
        'HTTP_FORBIDDEN': 403,
        'HTTP_METHOD_NOT_ALLOWED': 405,
        'HTTP_NOT_ACCEPTABLE': 406,
        'HTTP_NOT_IMPLEMENTED': 501,
        'HTTP_SERVICE_UNAVAILABLE': 503,
        'HTTP_BAD_REQUEST': 400,
        'HTTP_INTERNAL_SERVER_ERROR': 500,
    }

    def __getattr__(cls, key):
        try:
            return ReturnMetaClass.__return_codes[key]
        except KeyError:
            raise AttributeError("No return code {}".format(key))


class Return(object, metaclass=ReturnMetaClass):
    """
    This is a specialized class with constant members giving names to
    HTTP Status Codes. These members are:

      * ``Return.HTTP_OK``
      * ``Return.HTTP_MOVED_PERMANENTLY``
      * ``Return.HTTP_FOUND``
      * ``Return.HTTP_SEE_OTHER``
      * ``Return.HTTP_NOT_MODIFIED``
      * ``Return.HTTP_NOT_FOUND``
      * ``Return.HTTP_FORBIDDEN``
      * ``Return.HTTP_METHOD_NOT_ALLOWED``
      * ``Return.HTTP_NOT_ACCEPTABLE``
      * ``Return.HTTP_NOT_IMPLEMENTED``
      * ``Return.HTTP_SERVICE_UNAVAILABLE``
      * ``Return.HTTP_BAD_REQUEST``
      * ``Return.HTTP_INTERNAL_SERVER_ERROR``
    """
