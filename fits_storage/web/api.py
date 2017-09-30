from ..orm import NoResultFound
from ..orm.file import File
from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.program import Program
from ..orm.programpublication import ProgramPublication
from ..orm.publication import Publication
from ..orm.obslog_comment import ObslogComment

from ..utils.fitseditor import compare_cards, modify_multiple_cards
from ..utils.ingestqueue import IngestQueueUtil, IngestError
from ..utils.api import ApiProxy, ApiProxyError, NewCardsIncluded
from ..utils.null_logger import EmptyLogger
from ..utils.web import get_context, Return

from .user import needs_login

from ..fits_storage_config import storage_root
from ..fits_storage_config import magic_api_cookie, api_backend_location

from sqlalchemy import desc

from contextlib import contextmanager
import os
import pyfits as pf
import json
import fcntl
from time import strptime
from glob import iglob

from time import sleep

class RequestError(Exception):
    pass

class ItemError(Exception):
    def __init__(self, message, label=None):
        super(ItemError, self).__init__(message)
        self.label = label

class DummyLogger(object):
    def __getattr__(self, attr):
        return self

    def __call__(self, *args, **kw):
        return

def locked_file(path):
    fd = open(fullpath, "r+")
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    yield fd
    fd.close()

def get_json_data():
    try:
        return get_context().json
    except KeyError:
        raise RequestError("This looks like a malformed request. Cannot find the 'data' entry")
    except ValueError:
        raise RequestError("This looks like a malformed request. Invalid JSON")
    except TypeError:
        raise RequestError("This looks like a malformed request. Didn't get a string in data's content")

def lookup_diskfile(session, query):
    try:
        if 'data_label' in query:
            label = query['data_label']
            # There can be multiple files with the same data label when things go wrong with the observing system.
            # In that case, we pick the highest filename which will be the later file taken - which is usually the 
            # one that didn't fail.
            df = session.query(DiskFile).join(Header).filter(DiskFile.present == True).filter(Header.data_label == label).order_by(desc(DiskFile.filename)).first()
        elif 'filename' in query:
            label = query['filename']
            df = session.query(DiskFile).filter(DiskFile.present == True).filter(DiskFile.filename == label).one()
        else:
            raise ItemError("Expected 'data_label' or 'filename' to identify the item")
    except NoResultFound:
        raise ItemError("Could not find a file matching '{}'".format(label), label=label)

    return label, df

def error_response(message, id=None):
    response = {'result': False, 'error': message}
    if id is not None:
        response['id'] = id

    return response

qa_keywords = ('RAWGEMQA', 'RAWPIREQ')
qa_state_pairs = {
    'undefined': ('UNKNOWN', 'UNKNOWN'),
    'pass':      ('USABLE',  'YES'),
    'usable':    ('USABLE',  'NO'),
    'fail':      ('BAD',     'NO'),
    'check':     ('CHECK',   'CHECK'),
}

rs_keywords = ('RAWBG', 'RAWCC', 'RAWIQ', 'RAWWV')
"BG and WV it's 20, 50, 80, Any"
"CC: 50, 70, 80, Any"
"IQ: 20, 70, 85, Any"

rs_state_pairs = {
    'bg20':    ('20-percentile', None, None, None),
    'bg50':    ('50-percentile', None, None, None),
    'bg80':    ('80-percentile', None, None, None),
    'bgany':   ('Any', None, None, None),
    'cc50':    (None, '50-percentile', None, None),
    'cc70':    (None, '70-percentile', None, None),
    'cc80':    (None, '80-percentile', None, None),
    'ccany':   (None, 'Any', None, None),
    'iq20':    (None, None, '20-percentile', None),
    'iq70':    (None, None, '70-percentile', None),
    'iq85':    (None, None, '85-percentile', None),
    'iqany':   (None, None, 'Any', None),
    'wv20':    (None, None, None, '20-percentile'),
    'wv50':    (None, None, None, '50-percentile'),
    'wv80':    (None, None, None, '80-percentile'),
    'wvany':   (None, None, None, 'Any'),
}

def valid_pair(pair):
    return pair[1] is not None

class PairMapper(object):
    def __init__(self, keywords, pairs):
        self.kw = keywords
        self.pr = pairs

    def __call__(self, value):
        try:
            return filter(valid_pair, zip(self.kw, self.pr[value.lower()]))
        except KeyError:
            raise ValueError(value)

def map_release(value):
    # This will raise ValueError in vase of an illegal date
    strptime(value, "%Y-%m-%d")
    return [('RELEASE', value)]

def map_generic(value):
    return [tuple(value)]

change_actions = {
    'qa_state': PairMapper(qa_keywords, qa_state_pairs),
    'raw_site': PairMapper(rs_keywords, rs_state_pairs),
    'release':  map_release,
    'generic':  map_generic,
}

def map_changes(changes):
    change_pairs = []
    for key, value in changes.items():
        try:
            fn = change_actions[key]
        except KeyError:
            raise ItemError("Unknown action: {}".format(key))
        try:
            if isinstance(value, (tuple, list)):
                for v in value:
                    change_pairs.extend(fn(v))
            else:
                change_pairs.extend(fn(value))
        except ValueError:
            raise ItemError("Illegal value '{}' for action '{}'".format(value, key))

    return dict(change_pairs)

def process_update(session, proxy, query, iq):
    reingest = False
    label = None
    try:
        label, df = lookup_diskfile(session, query)
        filename = df.filename
        if not isinstance(query['values'], dict):
            return error_response("This looks like a malformed request: 'values' should be a dictionary", id=label)
        new_values = map_changes(query['values'])
        reject_new = query.get('reject_new', False)
        path = df.fullpath()
        # It seems like this isn't necessary and it's broken (call to one() with potentially multiple results)
        # reingest = iq.delete_inactive_from_queue(filename)
        # reingest = apply_changes(df, query['values']) or reingest
        reingest = proxy.set_image_metadata(path=path, changes=new_values, reject_new=reject_new)
        return {'result': True, 'id': label}
    except ItemError as e:
        return error_response(e.message, id=e.label)
    except KeyError as e:
        return error_response("This looks like a malformed request: 'values' does not exist", id=label)
    except IngestError as e:
        return error_response(e.message, id=label)
    except ApiProxyError as e:
        get_context().req.log(str(e))
        return error_response("An internal error occurred and your query could not be performed. It has been logged")
    except NewCardsIncluded:
        return error_response("Some of the keywords don't exist in the file", id=label)
    finally:
        if reingest:
           iq.add_to_queue(filename, os.path.dirname(path))

def process_all_updates(data):
    session = get_context().session
    proxy = ApiProxy(api_backend_location)
    iq = IngestQueueUtil(session, DummyLogger())
    for query in data:
        yield process_update(session, proxy, query, iq)

@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_cookie)], only_magic=True, content_type='json')
def update_headers():
    batch = True

    resp = get_context().resp
    resp.set_content_type('application/json')
    try:
        message = get_json_data()
        # The old format for the request was the list now in "request". This will provide compatibility
        # for both formats
        if isinstance(message, dict):
            # New format
            data = message['request']
            batch = message.get('batch', True)
        else:
            # Assume old format
            data = message
        if batch:
            resp.append_json(list(process_all_updates(data)))
        else:
            with resp.streamed_json() as stream:
                for chunk in process_all_updates(data):
                    stream.write(chunk)
    except RequestError as e:
        resp.append_json(error_response(e.message))
    except KeyError as e:
        resp.append_json(error_response("Malformed request. It lacks the '{}' argument".format(e.args[0])))
    except TypeError:
        resp.append_json(error_response("This looks like a malformed request. Expected a dictionary or a list of queries. Instead I got {}".format(type(data))))

def ingest_files():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'

    try:
        arguments = ctx.json
        file_pre  = arguments['filepre']
        path      = arguments['path']
        force     = arguments['force']
        force_md5 = arguments['force_md5']
    except ValueError:
        resp.append_json(error_response('Invalid information sent to the server'))
        return
    except KeyError as e:
        resp.append_json(error_response('Missing argument: {}'.format(e.args[0])))
        return

    # Assume that we're working on the local directory. There's no need for this
    # API entry in the S3 machine
    pattern = os.path.join(storage_root, path, file_pre)
    added = []

    logger = EmptyLogger()

    iq = IngestQueueUtil(ctx.session, logger)
    for i, entry in enumerate(iglob(pattern + '*'), 1):
        filename = os.path.basename(entry)
        iq.add_to_queue(filename, path, force=force, force_md5=force_md5)
        added.append(filename)

    if not added:
        resp.append_json(error_response('Could not find any file with prefix: {}*'.format(file_pre)))
    else:
        resp.append_json(dict(result=True, added=sorted(added)))

# TODO: "Only_magic" is a temporary thing. Check if it can stay
@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_cookie)], only_magic=True, content_type='json')
def ingest_programs():
    ctx = get_context()
    resp = ctx.resp
    resp.content_type = 'application/json'
    fields = ['reference', 'title', 'contactScientistEmail', 'abstrakt', 'piEmail', 'coIEmails', 'observations', 'investigatorNames']
    try:
        program = ctx.json
    except ValueError:
        resp.append_json(error_response('Invalid information sent to the server'))
        return

    session = ctx.session

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
            continue
        prog_pub = ProgramPublication(program = prog,
                                      publication = pub)
        session.add(prog_pub)

    session.commit()

@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_cookie)], only_magic=True, content_type='json')
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
