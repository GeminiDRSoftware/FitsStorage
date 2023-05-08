from fits_storage.queues.queue.fileopsqueue import FileOpsRequest, \
    FileOpsResponse


resp_ref_dict = {'ok': True,
                 'error': 'Nope',
                 'value': 'Hello, world'}

resp_ref_json = '{"ok": true, "error": "Nope", "value": "Hello, world"}'

req_ref_dict = {'request': 'Do thing',
                'args': {'do': 'this', 'one': 'thing'}}

req_ref_json = '{"request": "Do thing", "args": {"do": "this", "one": "thing"}}'


def test_foresp_init():
    r = FileOpsResponse()

    assert r.ok is False
    assert r.error == ''
    assert r.value == ''


def test_foresp_dict_json():
    r = FileOpsResponse()
    r.ok = True
    r.error = 'Nope'
    r.value = 'Hello, world'

    assert r.dict() == resp_ref_dict
    assert r.json() == resp_ref_json


def test_foresp_loads_json():
    r = FileOpsResponse()
    r.loads(resp_ref_json)

    assert r.dict() == resp_ref_dict
    assert r.json() == resp_ref_json


def test_foreq_init():
    r = FileOpsRequest()

    assert r.request == ''
    assert r.args == {}


def test_foreq_dict_json():
    r = FileOpsRequest()
    r.request = 'Do thing'
    r.args = {'do': 'this', 'one': 'thing'}

    assert r.dict() == req_ref_dict
    assert r.json() == req_ref_json


def test_foreq_loads_json():
    r = FileOpsRequest()
    r.loads(req_ref_json)

    assert r.dict() == req_ref_dict
    assert r.json() == req_ref_json
