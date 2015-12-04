'''
Module to deal with the interals of templating
'''

from jinja2 import Environment, FileSystemLoader
from ..fits_storage_config import template_root
from ..orm import session_scope
from functools import wraps
from mod_python import apache
from ..apache_return_codes import HTTP_SERVICE_UNAVAILABLE
from datetime import datetime

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

def bytes_to_GB(value, GiB=False):
    return int(value) / (1.0E9 if not GiB else 1073741824.0)

def format_float(value, decimals=2):
    try:
        return '{:.{pre}f}'.format(value, pre=decimals)
    except ValueError:
        return ''

def group_digits(value, decimals=0):
    try:
        if decimals > 0:
            return '{:,.{pre}f}'.format(value, pre=decimals)
        else:
            return '{:,}'.format(value)
    except ValueError:
        return ''

KB = 1024.0
MB = 1024 * KB
GB = 1024 * MB

def abbreviate_size(value):
    if value >= GB:
        return '{:.2f} GB'.format(value / GB)
    elif value >= MB:
        return '{:.2f} MB'.format(value / MB)
    else:
        return '{:.2f} KB'.format(value / KB)

class DateTimeObject(object):
    def __init__(self, when):
        self.when = when

    def __str__(self):
        if self.when == 'NOW':
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif self.when == 'TODAY':
            return str(datetime.now().date())
        else:
            return 'UNKNOWN DATE'

global_members = {
    'NOW': DateTimeObject('NOW'),
    'TODAY': DateTimeObject('TODAY')
}

custom_filters = {
    'datetime': datetime_filter,
    'seconds_since': seconds_since_filter,
    'throughput': bytes_per_second,
    'format_float': format_float,
    'bytes_to_GB': bytes_to_GB,
    'group_digits': group_digits,
    'abbreviate_size': abbreviate_size
}

included_extensions = [
    'jinja2.ext.with_'
]

def get_env():
    """Create a Jinja environment that includes our customizations"""
    jinja_env = Environment(loader=FileSystemLoader(template_root),
                            extensions=included_extensions,
    # When autoescape=False we assume that by default everything we output
    # is HTML-safe (no '<', no '>', no '&', ...)
    # This may be too much of an assumption, BUT... performance is better
                            autoescape=False)
#                            autoescape=True)

    jinja_env.globals.update(global_members)
    jinja_env.filters.update(custom_filters)

    return jinja_env

class SkipTemplateError(Exception):
    """Exception to be raised when we need to skip the rendering of a template.

       A numeric status code has to be provided"""
    def __init__(self, status):
        self.status = status

class TemplateAccessError(Exception):
    pass

class InterruptedError(Exception):
    pass

# This is a decorator for functions that use templates. Simplifies
# some use cases, making it easy to return from the function at
# any point without having to care about repeating the content generation
# at every single exit point.
def templated(template_name, content_type="text/html", with_generator=False, with_session=False, default_status=apache.HTTP_OK, at_end_hook=None):
    """``template_name`` is the path to the template file, relative to the ``template_root``.

       If ``with_generator`` is ``True``, Jinja2 will be instructed to try to chunk the output,
       sending info back to the client as soon as possible.

       If ``with_session`` is ``True``, we keep the whole operation within a session scope (the
       session object is passed as first argument to the decorated function). This allows
       the function to return ORM objects that can be manipulated by the template, without
       having to detach them from the session first. A typical use case is to pass a query
       so that the template iterates over it.

       If ``at_end_hook`` is defined, it has to be a callable object. It will be invoked after
       the template has generated all the text, passing both the session object and the
       request.
    """
    def template_decorator(fn):
        @wraps(fn)
        def fn_wrapper(req, *args, **kw):
            try:
                template = get_env().get_template(template_name)
            except IOError:
                raise TemplateAccessError(template_name)

            with session_scope() as session:
                req.content_type = content_type
                try:
                    if with_session:
                        context = fn(session, req, *args, **kw)
                    else:
                        context = fn(req, *args, **kw)
                    if isinstance(context, tuple):
                        status, context = context
                    else:
                        status = default_status

                    if not with_generator:
                        req.write(template.render(context))
                    else:
                        for text in template.generate(context):
                            req.write(text)

                    if at_end_hook:
                        at_end_hook(session, req)
                except SkipTemplateError as e:
                    status = e.status
                except IOError:
                    # Assume that his means we got an interrupted connection
                    # (eg. the user stopped the query on their browser)
                    raise InterruptedError

                return status
        return fn_wrapper
    return template_decorator
