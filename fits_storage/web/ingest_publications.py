
from fits_storage.server.orm.program import Program
from fits_storage.server.orm.programpublication import ProgramPublication
from fits_storage.server.orm.publication import Publication

from .user import needs_login

from fits_storage.server.wsgi.context import get_context

from fits_storage.config import get_config
fsc = get_config()

def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id

    return response


def process_publication(pub_data):
    bibcode = pub_data['bibcode']

    ctx = get_context()
    session = ctx.session

    pub = session.query(Publication)\
                 .filter(Publication.bibcode == bibcode)\
                 .first()
    if pub is None:
        pub = Publication(bibcode)
        session.add(pub)

    pub_fields = ('author', 'title', 'year', 'journal', 'telescope',
                  'instrument', 'country', 'wavelength', 'mode', 'too',
                  'partner')

    for field in pub_fields:
        setattr(pub, field, pub_data.get(field))

    for bfield in ('gstaff', 'gsa', 'golden'):
        value = pub_data.get(bfield)
        if str(value).upper() in 'YN':
            setattr(pub, bfield, value.upper() == 'Y')
        else:
            setattr(pub, bfield, None)

    prog_dict = dict((pp.program_text_id, pp) for pp in pub.publication_programs)
    cur_programs = set(prog_dict)
    sent_programs = set(pub_data.get('programs', []))

    # Remove existing associations with programs that are not in sent set
    for progid in (cur_programs - sent_programs):
        session.delete(prog_dict[progid])

    # Create new associations
    for progid in (sent_programs - cur_programs):
        prog = session.query(Program)\
                      .filter(Program.program_id == progid)\
                      .first()
        if prog is None:
            prog_pub = ProgramPublication(program=None,
                                          publication=pub,
                                          program_text_id=progid)
        else:
            prog_pub = ProgramPublication(program=prog,
                                          publication=pub)
        session.add(prog_pub)

    session.commit()


@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_server_cookie)], only_magic=True, content_type='json')
def ingest_publications():
    ctx = get_context()
    ctx.resp.set_content_type('application/json')

    try:
        arguments = ctx.json
    except ValueError:
        ctx.resp.append_json(error_response('Invalid information sent to the server'))
        return

    if arguments['single']:
        try:
            process_publication(arguments['payload'])
        except KeyError:
            ctx.resp.append_json(error_response('Missing bibcode!'))
        ctx.resp.append_json(dict(result=True))
    else:
        collect_res = []
        for pub in arguments['payload']:
            try:
                process_publication(pub)
            except KeyError:
                collect_res
        if all(collect_res):
            ctx.resp.append_json(dict(result=True))
        else:
            ret = error_response('Missing bibcode!')
            ret['pubs'] = collect_res
            ctx.resp.append_json(ret)
