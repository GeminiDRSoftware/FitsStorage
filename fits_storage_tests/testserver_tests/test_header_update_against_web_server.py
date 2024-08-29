# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.

import http
import requests

from fits_storage_tests.testserver_tests.helpers import getserver
from fits_storage_tests.testserver_tests.helpers import _ensureuploaded, \
    _waitforingest


def test_update(tmp_path):
    server = getserver()
    filename = 'N20180329S0134.fits.bz2'
    _ensureuploaded(tmp_path, filename, '6a9688a89307afa7776bd23ea4ccae3f')

    print("Testing old format header update by filename")
    # Create the header_update payload, old format
    msg = '[{"filename": "N20180329S0134.fits", ' \
          '"values": {"qa_state": "Usable"}}]'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '[{"result": true, "id": "N20180329S0134.fits"}]'

    js = _waitforingest(filename, data_md5='d5f5b0c59ac7ba904d98ca1f343254a4')
    jsf = js[0]
    assert jsf['qa_state'] == 'Usable'

    print("Testing old format header update by datalabel")
    # Create the header_update payload, old format
    msg = '[{"data_label": "GN-2018A-FT-103-13-003", ' \
          '"values": {"raw_site": "iqany"}}]'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": true, "id": "GN-2018A-FT-103-13-003"}]'

    js = _waitforingest(filename, data_md5='65a9baa43115b07d60b6d1eae73de0a5')
    jsf = js[0]
    assert jsf['raw_iq'] == 100

    print("Testing new format header update by datalabel")
    # Create the header_update payload, old format
    msg = '{"request": [{"data_label": "GN-2018A-FT-103-13-003", ' \
          '"values": {"release": "2123-04-05"}, ' \
          '"reject_new": true}], "batch": false}'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": true, "id": "GN-2018A-FT-103-13-003"}]'

    js = _waitforingest(filename, data_md5='3c709f93be1bf20f6c59f64aa075b0cf')
    jsf = js[0]
    assert jsf['release'] == '2123-04-05'

    print("Testing new format header update by filename")
    # Create the header_update payload, old format
    msg = '{"request": [{"filename": "N20180329S0134.fits", ' \
          '"values": {"raw_site": "iq70"}, "reject_new": true}], ' \
          '"batch": false}'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": true, "id": "N20180329S0134.fits"}]'
    js = _waitforingest(filename, data_md5='16c3e280a405726ee62ea5ad18f3d03c')
    jsf = js[0]
    assert jsf['raw_iq'] == 70

    print("Testing new format bad request")
    # Create the header_update payload, old format
    msg = '{"request": [{"typo": "N20180329S0134.fits", ' \
          '"values": {"raw_site": "iqany"}, "reject_new": true}], ' \
          '"batch": false}'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": false, ' \
                       '"error": "No filename or data_label given"}]'

    print("Testing non-existent file")
    msg = '[{"filename": "N20991231S9999.fits", ' \
          '"values": {"qa_state": "Usable"}}]'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '[{"result": false, "error": "No present file found ' \
                       'for filename or datalabel", "id": ' \
                       '"N20991231S9999.fits"}]'


def test_multi_update_new(tmp_path):
    server = getserver()
    filename1 = 'N20180329S0134.fits.bz2'
    filename2 = 'N20180524S0117.fits.bz2'
    _ensureuploaded(tmp_path, filename1, '6a9688a89307afa7776bd23ea4ccae3f')
    _ensureuploaded(tmp_path, filename2, '760f4ab19268b8171e4cfd12a71ba4b4')

    print("Testing new format multi header update by filename")
    # Create the header_update payload, new format
    msg = '{"request": [' \
          '{"filename": "N20180329S0134.fits", ' \
          '"values": {"raw_site": "iq70"}, ' \
          '"reject_new": true},' \
          '{"filename": "N20180524S0117.fits", ' \
          '"values": {"raw_site": "iq20"}, ' \
          '"reject_new": true}], ' \
          '"batch": false}'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": true, "id": "N20180329S0134.fits"}, ' \
                       '{"result": true, "id": "N20180524S0117.fits"}]'
    js = _waitforingest(filename1, data_md5='3e24bd3a4c1171ae7b76ccba9d1c8f53')
    jsf = js[0]
    assert jsf['raw_iq'] == 70
    js = _waitforingest(filename2, data_md5='2375eea8e4448f3d1fbbd7c1a1a2f4a6')
    jsf = js[0]
    assert jsf['raw_iq'] == 20


def test_multi_update_old(tmp_path):
    server = getserver()
    filename1 = 'N20180329S0134.fits.bz2'
    filename2 = 'N20180524S0117.fits.bz2'
    _ensureuploaded(tmp_path, filename1, '6a9688a89307afa7776bd23ea4ccae3f')
    _ensureuploaded(tmp_path, filename2, '760f4ab19268b8171e4cfd12a71ba4b4')

    print("Testing new format multi header update by filename")
    # Create the header_update payload, old format
    msg = '[{"filename": "N20180329S0134.fits", ' \
          '"values": {"qa_state": "Check"}}, ' \
          '{"filename": "N20180524S0117.fits", ' \
          '"values": {"qa_state": "Check"}}]'
    url = f"{server}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/json'
    assert req.text == '[{"result": true, "id": "N20180329S0134.fits"}, ' \
                       '{"result": true, "id": "N20180524S0117.fits"}]'
    js = _waitforingest(filename1, data_md5='96a93e468c647ac857d8033c4bc52b71')
    jsf = js[0]
    assert jsf['qa_state'] == 'CHECK'
    js = _waitforingest(filename2, data_md5='a941005aaea2fc51ca2c665c4a7f43a5')
    jsf = js[0]
    assert jsf['qa_state'] == 'CHECK'
