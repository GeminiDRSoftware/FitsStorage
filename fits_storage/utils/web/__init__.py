from adapter import Context, context_wrapped
try:
    from mod_python_adapter import Request as ModPythonRequest
except ImportError:
    # Will happen if mod_python is not installed...
    pass
