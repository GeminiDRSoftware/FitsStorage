"""
Very simple "debug page" response
"""

import sys
from pprint import pformat

from fits_storage.server.wsgi.context import get_context

debug_template = """
Debug info

Python path:
{path}

uri: {uri}

Environment:
{env}
"""


# Send debugging info to browser
def debugmessage():
    ctx = get_context()
    req, resp = ctx.req, ctx.resp

    resp.content_type = 'text/plain'

    resp.append(debug_template.format(
        path='\n'.join('-- {}'.format(x) for x in sys.path),
        uri=req.env.uri,
        env=pformat(req.env._env)
    ))
