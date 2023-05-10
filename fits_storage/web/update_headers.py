import json
import os
import fcntl

from time import strptime

from sqlalchemy.exc import NoResultFound
from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile

from ..utils.web import get_context, Return

from .user import needs_login

from ..fits_storage_config import storage_root, magic_api_server_cookie
from ..fits_storage_config import magic_api_cookie, api_backend_location

from sqlalchemy import desc, or_


class RequestError(Exception):
    pass


class ItemError(Exception):
    def __init__(self, message, label=None):
        super(ItemError, self).__init__(message)
        self.label = label


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
            # There can be multiple files with the same data label when things
            # go wrong with the observing system. In that case, we pick the
            # highest filename which will be the later file taken - which is
            # usually the one that didn't fail.
            df = session.query(DiskFile).join(Header).filter(DiskFile.present == True).filter(Header.data_label == label).order_by(desc(DiskFile.filename)).first()
        elif 'filename' in query:
            label = query['filename']
            print(f"looking for file by name {label}")
            if label is not None and not label.endswith('.bz2'):
                label2 = f"{label}.bz2"
                df = session.query(DiskFile).filter(DiskFile.present == True) \
                    .filter(or_(DiskFile.filename == label,
                                DiskFile.filename == label2)).one()
            else:
                df = session.query(DiskFile).filter(DiskFile.present == True).filter(DiskFile.filename == label).one()
        else:
            print("no filename field in query to search on")
            raise ItemError("Expected 'data_label' or 'filename' to identify the item")
    except NoResultFound:
        print(f"no diskfile found for {label}")
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
            return list(filter(valid_pair, list(zip(self.kw, self.pr[value.lower()]))))
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
    for key, value in list(changes.items()):
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
    print("in api.process_update")
    reingest = False
    label = None
    header_fields = None
    try:
        label, df = lookup_diskfile(session, query)
        if df is None:
            print("Got none diskfile from lookup")
        filename = df.filename
        print(f"Saw filename from lookup of {filename}")
        if not isinstance(query['values'], dict):
            return error_response("This looks like a malformed request: 'values' should be a dictionary", id=label)
        new_values = map_changes(query['values'])
        reject_new = query.get('reject_new', False)
        path = df.fullpath()
        # It seems like this isn't necessary and it's broken (call to one() with potentially multiple results)
        # reingest = iq.delete_inactive_from_queue(filename)
        # reingest = apply_changes(df, query['values']) or reingest
        md5_before_header = df.get_file_md5()
        print("calling set_image_metadata")
        print("  new_values: %s" % new_values)
        print("  reject_new: %s" % reject_new)
        print("  path: %s" % path)
        print("  md5_before_header: %s" % md5_before_header)
        reingest, md5_after_header = proxy.set_image_metadata(path=path, changes=new_values, reject_new=reject_new)
        print("done calling proxy.set_image_metadata")
        if reingest:
            print("reingest was true...")
            header_fields = new_values
            # md5_after_header = df.get_file_md5()
            print("header_fields: %s" % header_fields)
            print("md5_after_header: %s" % md5_after_header)
        else:
            print("reingest was false")
        return {'result': True, 'id': label, 'md5': md5_after_header}
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
            print("reingest was True, adding updated file to queue")
            print(f"  filename: {filename}\n"
                  f"  header_fields: {header_fields}\n"
                  f"  md5_before: {md5_before_header}\n"
                  f"  md5_after: {md5_after_header}\n")
            try:
                iq.add_to_queue(filename, os.path.dirname(path), header_fields=json.dumps(header_fields),
                                md5_before_header=md5_before_header, md5_after_header=md5_after_header,
                                reject_new=reject_new)
            except:
                print("Error occured during iq.add_to_queue")
            print("done adding header update to queue")


def process_all_updates(data):
    print("in api.process_all_updates")
    session = get_context().session
    proxy = ApiProxy(api_backend_location)
    iq = IngestQueueUtil(session, DummyLogger())
    for query in data:
        print("handling query: %s" % query)
        yield process_update(session, proxy, query, iq)


@needs_login(magic_cookies=[('gemini_api_authorization', magic_api_server_cookie)], only_magic=True, content_type='json')
def update_headers():
    print("in api.update_headers")
    batch = True

    resp = get_context().resp
    resp.set_content_type('application/json')
    try:
        message = get_json_data()
        print("got message:\n%s" % message)
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
                print("at process_all_updates")
                for chunk in process_all_updates(data):
                    stream.write(chunk)
                print("done process_all_updates")
    except RequestError as e:
        resp.append_json(error_response(e.message))
        resp.status = Return.HTTP_INTERNAL_SERVER_ERROR
    except KeyError as e:
        resp.append_json(error_response("Malformed request. It lacks the '{}' argument".format(e.args[0])))
        resp.status = Return.HTTP_BAD_REQUEST
    except TypeError:
        resp.append_json(error_response("This looks like a malformed request. Expected a dictionary or a list of queries. Instead I got {}".format(type(data))))
        resp.status = Return.HTTP_BAD_REQUEST
