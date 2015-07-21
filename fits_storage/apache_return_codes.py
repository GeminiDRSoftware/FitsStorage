"""
This is an "Apache Proxy" module, that can be imported in place of
mod_python.apache. If we're not in local mode, it import from the real mod_python
apache module, if we are in local mode, it provides various constants etc
(eg apache.OK) that are used in various functions, primarily as return values
from functions that generate html and are called directly by the apache
request handler.
Importing this module allows use of those functions without apache mod_python
The module is only actually expected to import correctly if we are running
inside apache, so command line scripts using this will always need the local
definitions anyway.
"""

from fits_storage_config import using_apache

if using_apache:
    try:
        from mod_python.apache import OK, HTTP_OK, HTTP_NOT_FOUND, HTTP_FORBIDDEN, HTTP_NOT_ACCEPTABLE
        from mod_python.apache import HTTP_NOT_IMPLEMENTED, HTTP_SERVICE_UNAVAILABLE, HTTP_BAD_REQUEST
        from mod_python.apache import REMOTE_NOLOOKUP
        define_locally = False
    except ImportError:
        define_locally = True
else:
    define_locally = True


if define_locally:
    OK = 200
    HTTP_OK = 200
    HTTP_NOT_FOUND = 404
    HTTP_FORBIDDEN = 403
    HTTP_NOT_ACCEPTABLE = 406
    HTTP_NOT_IMPLEMENTED = 501
    HTTP_SERVICE_UNAVAILABLE = 503
    HTTP_BAD_REQUEST = 400
    REMOTE_NOLOOKUP = None
