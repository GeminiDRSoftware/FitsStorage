import datetime

import pytest

import urllib
import json
import copy

from fits_storage.web.calmgr import parse_post_calmgr_inputs, SkipTemplateError

class MockUsageLog(object):
    note = None
    def add_note(self, arg):
        self.note = arg

class MockContext(object):
    pass

ctx = MockContext()
ctx.usagelog = MockUsageLog()
ctx.raw_data = None

def test_None():
    ctx.raw_data = None
    with pytest.raises(SkipTemplateError) as ste:
       parse_post_calmgr_inputs(ctx)

    assert ctx.usagelog.note == 'Missing POST data'

def test_old():
    types = ['GMOS', 'PREPARED']
    descr = {'filter': 'r', 'exptime': 3.0}
    sequence = [("descriptors", descr), ("types", types)]
    ctx.raw_data = urllib.parse.urlencode(sequence).encode('utf-8')

    d, t = parse_post_calmgr_inputs(ctx)

    assert t == ['GMOS', 'PREPARED']
    assert d == {'filter': 'r', 'exptime': 3.0}

def test_json():
    types = ['GMOS', 'PREPARED']
    ut_datetime = datetime.datetime(2020, 1, 23, 1,23,45)
    descr = {'filter': 'r', 'exptime': 3.0, 'ut_datetime': ut_datetime}

    descr_tosend = copy.copy(descr)
    descr_tosend['ut_datetime'] = descr_tosend['ut_datetime'].isoformat()

    payload = {'tags': types, 'descriptors': descr_tosend}
    jsontext = json.dumps(payload)

    ctx.raw_data = jsontext.encode('utf-8')

    d, t = parse_post_calmgr_inputs(ctx)

    assert t == ['GMOS', 'PREPARED']
    for item in descr:
        assert d[item] == descr[item]
