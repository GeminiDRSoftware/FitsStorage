from fits_storage.queues.queue.fileopsqueue import FileOpsResponse

# for = FileOpsResponse

ref_dict = {'ok': True,
            'error': 'Nope',
            'value': 'Hello, world'}

ref_json = '{"ok": true, "error": "Nope", "value": "Hello, world"}'

def test_for_init():
    r = FileOpsResponse()

    assert r.ok is False
    assert r.error == ''
    assert r.value == ''

def test_for_dict_json():
    r = FileOpsResponse()
    r.ok = True
    r.error = 'Nope'
    r.value = 'Hello, world'

    assert r.dict() == ref_dict
    assert r.json() == ref_json

def test_for_loads_json():
    r = FileOpsResponse()
    r.loads(ref_json)

    assert r.dict() == ref_dict
    assert r.json() == ref_json