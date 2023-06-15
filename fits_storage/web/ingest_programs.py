

from fits_storage.server.orm.program import Program
from fits_storage.server.orm.obslog_comment import ObslogComment

from fits_storage.server.wsgi.context import get_context

from .user import needs_cookie

from fits_storage.config import get_config
fsc = get_config()

def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id

    return response


# TODO: "Only_magic" is a temporary thing. Check if it can stay
@needs_cookie(magic_cookies=
              [('gemini_api_authorization', fsc.magic_api_server_cookie)],
              content_type='json')
def ingest_programs():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'
    fields = ['reference', 'title', 'contactScientistEmail', 'abstrakt', 'piEmail', 'coIEmails', 'observations',
              'investigatorNames', 'too']
    try:
        programs = ctx.json
        if not isinstance(programs, list):
            programs = [programs, ]
    except ValueError:
        resp.append_json(error_response('Invalid information sent to the server'))
        return

    session = ctx.session

    for program in programs:
        prog_obj = session.query(Program).filter(Program.program_id == program['reference']).first()
        if prog_obj is None:
            prog_obj = Program(program['reference'])
            session.add(prog_obj)

        pairs = (('title', 'title'),
                 ('abstrakt', 'abstract'),
                 ('piEmail', 'piemail'),
                 ('coIEmails', 'coiemail'),
                 ('investigatorNames', 'pi_coi_names'))

        for remote, local in pairs:
            try:
                setattr(prog_obj, local, program[remote])
            except KeyError:
                # Just ignore any non-existing associationsfield
                pass
        if 'too' in program and program['too'].lower() in ('standard', 'rapid'):
            prog_obj.too = True
            print(f"Set TOO to True for {program['reference']}")
        else:
            prog_obj.too = False

        for obs in program['observations']:
            lcomms = session.query(ObslogComment).filter(ObslogComment.data_label == obs['label']).first()
            if lcomms is None:
                lcomms = ObslogComment(program['reference'], obs['label'], obs['comment'])
                session.add(lcomms)
            else:
                lcomms.program_id = program['reference']
                lcomms.comment = obs['comment']

    session.commit()

    resp.append_json(dict(result=True))
