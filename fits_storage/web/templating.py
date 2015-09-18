'''
Module to deal with the interals of templating
'''

from jinja2 import Environment, FileSystemLoader
from ..fits_storage_config import template_root
from ..orm import session_scope
from functools import wraps
from mod_python import apache

def datetime_filter(value, format=None, chopped=False):
    if format=='full':
        fmt = "%Y-%m-%d %H:%M:%S.%f"
    elif format=='date':
        fmt = "%Y-%m-%d"
    else:
        fmt = "%Y-%m-%d %H:%M:%S"

    res = value.strftime(fmt)

    return res if not chopped else res[:21]

def seconds_since_filter(value, since, formatted=True):
    if not (value and since):
        return '' if formatted else None

    ret = (value - since).total_seconds()

    if formatted:
        return '{:.2f}'.format(ret)
    return ret

def bytes_per_second(value, time, divider=1000000.0):
    try:
        return '{:.2f}'.format((value / time) / divider)
    except (ZeroDivisionError, TypeError):
        return ''

def bytes_to_GB(value):
    return int(value) / 1.0E9


def format_float(value, decimals=2):
    try:
        return '{:.{pre}f}'.format(value, pre=decimals)
    except ValueError:
        return ''

def get_env():
    jinja_env = Environment(loader=FileSystemLoader(template_root),
                            extensions=['jinja2.ext.with_'],
    # When autoescape=False we assume that by default everything we output
    # is HTML-safe (no '<', no '>', no '&', ...)
    # This may be too much of an assumption, BUT... performance is better
                            autoescape=False)
#                            autoescape=True)

    jinja_env.filters['datetime'] = datetime_filter
    jinja_env.filters['seconds_since'] = seconds_since_filter
    jinja_env.filters['throughput'] = bytes_per_second
    jinja_env.filters['format_float'] = format_float
    jinja_env.filters['bytes_to_GB'] = bytes_to_GB

    return jinja_env

# This is a decorator for functions that use templates. Simplifies
# some use cases, making it easy to return from the function at
# any point without having to care about repeating the content generation
# at every single exit point.
def templated(template_name, content_type="text/html", with_generator=False, with_session=False, default_status=apache.HTTP_OK):
    """template_name is the path to the template file, relative to the template_root.

       If with_generator is True, Jinja2 will be instructed to try to chunk the output,
       sending info back to the client as soon as possible.

       If with_session is true, we keep the whole operation within a session scope (the
       session object is passed as first argument to the decorated function). This allows
       the function to return ORM objects that can be manipulated by the template, without
       having to detach them from the session first. A typical use case is to pass a query
       so that the template iterates over it.
    """
    def template_decorator(fn):
        @wraps(fn)
        def fn_wrapper(req, *args, **kw):
            template = get_env().get_template(template_name)
            with session_scope() as session:
                if with_session:
                    context = fn(session, req, *args, **kw)
                else:
                    context = fn(req, *args, **kw)
                if isinstance(context, tuple):
                    status, context = context
                else:
                    status = default_status

                req.content_type = content_type
                if not with_generator:
                    req.write(template.render(context))
                else:
                    for text in template.generate(context):
                        req.write(text)
                return status
        return fn_wrapper
    return template_decorator
