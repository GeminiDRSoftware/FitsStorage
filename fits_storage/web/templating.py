'''
Module to deal with the interals of templating
'''

from jinja2 import Environment, FileSystemLoader
from ..fits_storage_config import template_path

def get_env():
    jinja_env = Environment(loader=FileSystemLoader(template_path),
    # When autoescape=False we assume that by default everything we
    # is HTML-safe (no '<', no '>', no '&', ...)
    # This may be too much of an assumption, BUT... performance is better
                            autoescape=False)
#                            autoescape=True)

    return jinja_env
