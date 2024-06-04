from fits_storage.server.odb_data_handlers import update_programs

from fits_storage.server.wsgi.context import get_context

from .user import needs_cookie

from fits_storage.config import get_config
fsc = get_config()


def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id
    return response


@needs_cookie(magic_cookies=
              [('gemini_fits_upload_auth', fsc.upload_auth_cookie)],
              content_type='json')
def ingest_programs():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'

    try:
        programs = ctx.json
        if not isinstance(programs, list):
            programs = [programs]
    except ValueError:
        resp.append_json(error_response('Invalid data sent to the server'))
        return

    session = ctx.session

    # This also updates the obslog comments.
    # It does not update notifications though, that could be trivially added
    # here if desired.
    update_programs(session, programs)

    resp.append_json(dict(result=True))
