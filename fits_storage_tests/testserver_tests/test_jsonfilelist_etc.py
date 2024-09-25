# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it.


import requests
import http

from fits_storage_tests.testserver_tests.helpers import getserver
from fits_storage_tests.testserver_tests.helpers import _ensureuploaded


def test_jsonfilelist(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    server = getserver()
    url = f"{server}/jsonfilelist/present/{filename}"
    req = requests.get(url, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    j = req.json()
    assert isinstance(j, list)
    assert len(j) == 1
    j = j[0]
    assert j['name'] == filename.removesuffix('.bz2')
    assert j['filename'] == filename
    assert j['path'] == ''
    assert j['compressed'] is True
    # Don't be too sensitive here as these could change with header updates
    # and this file is used in the header update tests
    assert j['file_size'] > 0
    assert j['data_size'] > 0
    assert len(j['file_md5']) == 32
    assert len(j['data_md5']) == 32


def test_jsonqastate(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    server = getserver()
    url = f"{server}/jsonqastate/present/{filename}"
    req = requests.get(url, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    j = req.json()
    assert isinstance(j, list)
    assert len(j) == 1
    j = j[0]
    assert j['data_label'] == 'GN-2018A-FT-103-13-003'
    assert j['filename'] == filename
    assert len(j['data_md5']) == 32
    assert j['qa_state'] in ('Pass', 'Fail', 'Usable', 'CHECK', 'Undefined')


def test_xmlfilelist(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    server = getserver()
    url = f"{server}/xmlfilelist/present/{filename}"
    req = requests.get(url, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'text/xml'
    # This is a bit lame, but a sanity check for now
    assert len(req.text) > 200


def test_jsonsummary(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    server = getserver()
    url = f"{server}/jsonsummary/present/{filename}"
    req = requests.get(url, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    j = req.json()
    assert isinstance(j, list)
    assert len(j) == 1
    j = j[0]
    assert j['data_label'] == 'GN-2018A-FT-103-13-003'
    assert j['filename'] == filename
    assert len(j['data_md5']) == 32
    assert j['qa_state'] in ('Pass', 'Fail', 'Usable', 'CHECK', 'Undefined')
    assert j['instrument'] == 'NIRI'
    assert j['disperser'] == 'MIRROR'
    assert j['release'] == '2018-09-29'


def test_jsonsummary_with_photstandard(tmp_path):
    filename = 'N20220801S0045.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    server = getserver()
    url = f"{server}/jsonsummary/present/{filename}"
    req = requests.get(url, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    j = req.json()
    assert isinstance(j, list)
    assert len(j) == 1
    j = j[0]
    assert j['data_label'] == 'GN-CAL20220801-3-001'
    assert j['phot_standard'] is True
    assert isinstance(j['phot_standards'], list)
    psl = j['phot_standards']
    assert len(psl) == 5
