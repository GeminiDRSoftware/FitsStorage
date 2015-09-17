'''
Module to deal with the interals of templating
'''

from jinja2 import Environment, FileSystemLoader
from ..fits_storage_config import template_path
from ..orm import session_scope
from functools import wraps
from mod_python import apache

def get_env():
    jinja_env = Environment(loader=FileSystemLoader(template_path),
    # When autoescape=False we assume that by default everything we
    # is HTML-safe (no '<', no '>', no '&', ...)
    # This may be too much of an assumption, BUT... performance is better
                            autoescape=False)
#                            autoescape=True)

    return jinja_env

# This is a decorator for functions that use templates. Simplifies
# some use cases, making it easy to return from the function at
# any point
def templated(template_name, content_type="text/html", with_generator=False, with_session=False, default_status=apache.HTTP_OK):
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
