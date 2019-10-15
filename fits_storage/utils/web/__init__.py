from .adapter import get_context, Return, RequestRedirect, ClientError
from .adapter import context_wrapped, with_content_type
try:
    from .mod_python_adapter import Request as ModPythonRequest, Response as ModPythonResponse
except ImportError:
    # Will happen if mod_python is not installed...
    pass

try:
    from .wsgi_adapter import Request as WSGIRequest, Response as WSGIResponse, ArchiveContextMiddleware
except ImportError:
    # This should never happen, but just in case...
    pass
