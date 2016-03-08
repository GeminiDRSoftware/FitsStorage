from thread import get_ident
from functools import wraps

def context_wrapped(fn):
    @wraps(fn)
    def wrapper(*args, **kw):
        ctx = Context()
        try:
            return fn(ctx, *args, **kw)
        finally:
            ctx.invalidate()
    return wrapper

class Context(object):
    __threads = {}
    def __new__(cls):
        this = get_ident()
        try:
            ret = Context.__threads[this]
            if not ret._valid:
                ret = Context.__threads[this] = object.__new__(cls)
        except KeyError:
            ret = Context.__threads[this] = object.__new__(cls)

        return ret

    def __init__(self):
        self._valid  = True

    def invalidate(self):
        self._valid = False

class Response(object):
    pass
