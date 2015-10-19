from ..orm          import session_scope, NoResultFound
from ..orm.file     import File
from ..orm.header   import Header
from ..orm.diskfile import DiskFile

from ..utils.fitseditor import compare_cards, modify_multiple_cards
from ..utils.ingestqueue import IngestQueueUtil, IngestError
from ..utils.api import ApiProxy, ApiProxyError

from .user import needs_login

from ..fits_storage_config import magic_api_cookie, api_backend_location

from mod_python import apache, util
from contextlib import contextmanager
import os
import pyfits as pf
import json
import fcntl

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

def get_json_data(req):
    try:
        return json.loads(req.read())
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
            df = session.query(DiskFile).join(Header).filter(DiskFile.present == True).filter(Header.data_label == label).one()
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
    'Undefined': ('UNKNOWN', 'UNKNOWN'),
    'Pass':      ('USABLE',  'YES'),
    'Usable':    ('USABLE',  'NO'),
    'Fail':      ('BAD',     'NO'),
    'Check':     ('CHECK',   'CHECK'),
}

change_actions = {
    'qa_state': (qa_keywords, qa_state_pairs),
}

def map_changes(changes):
    change_pairs = []
    for key, value in changes.items():
        try:
            keywords, pairs = change_actions[key]
        except KeyError:
            raise ItemError("Unknown action: {}".format(key))

        try:
            change_pairs.extend(zip(keywords, pairs[value]))
        except KeyError:
            raise ItemError("Illegal value '{}' for action '{}'".format(value, key))

    return dict(change_pairs)

@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_cookie)], content_type='json')
def update_headers(req):
    with session_scope() as session:
        iq = IngestQueueUtil(session, DummyLogger())
        proxy = ApiProxy(api_backend_location)
        try:
            data = get_json_data(req)
            response = []
            reingest = False
            for query in data:
                label = None
                try:
                    label, df = lookup_diskfile(session, query)
                    filename = df.filename
                    if not isinstance(query['values'], dict):
                        response.append(error_response("This looks like a malformed request: 'values' should be a dictionary", id=label))
                        continue
                    new_values = map_changes(query['values'])
                    path = df.fullpath()
                    reingest = iq.delete_inactive_from_queue(filename)
                    # reingest = apply_changes(df, query['values']) or reingest
                    reingest = proxy.set_image_metadata(path=path, changes=new_values)
                    response.append({'result': True, 'id': label})
                except ItemError as e:
                    response.append(error_response(e.message, id=e.label))
                except KeyError as e:
                    response.append(error_response("This looks like a malformed request: 'values' does not exist", id=label))
                except IngestError as e:
                    response.append(error_response(e.message, id=label))
                except ApiProxyError:
                    # TODO: Actually log this and tell someone about it...
                    response.append(error_response("An internal error ocurred and your query could not be performed. It has been logged"))
                finally:
                    if reingest:
                       iq.add_to_queue(filename, os.path.dirname(path))
        except RequestError as e:
            response = error_response(e.message)
        except TypeError:
            response = error_response("This looks like a malformed request. Expected a list of queries. Instead I got {}".format(type(data)))

    req.content_type = 'application/json'
    req.write(json.dumps(response))

    return apache.HTTP_OK
