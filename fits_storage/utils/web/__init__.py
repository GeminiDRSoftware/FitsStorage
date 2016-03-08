from adapter import Context, context_wrapped, with_content_type
try:
    from mod_python_adapter import Request as ModPythonRequest, Response as ModPythonResponse
except ImportError:
    # Will happen if mod_python is not installed...
    pass
