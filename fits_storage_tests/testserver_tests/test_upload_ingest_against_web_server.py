# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.

import http
import requests


from fits_storage_tests.testserver_tests.helpers import getserver
from fits_storage_tests.testserver_tests.helpers import _ensureuploaded, \
    _uploadfile, _waitforingest


def test_upload(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    resp = _uploadfile(tmp_path, filename)

    assert resp.status_code == http.HTTPStatus.OK
    assert resp.text == '[{"filename": "N20180329S0134.fits.bz2", ' \
                        '"size": 1059693, ' \
                        '"md5": "1c1c2eb66af5a49218ea95a53b2b9f78"}]'

    js = _waitforingest(filename, data_md5='6a9688a89307afa7776bd23ea4ccae3f')
    assert js is not None

    # Sanity check initial values
    jsf = js[0]
    assert jsf['filename'] == filename
    assert jsf['data_md5'] == '6a9688a89307afa7776bd23ea4ccae3f'
    assert jsf['release'] == '2018-09-29'
    assert jsf['raw_iq'] == 85
    assert jsf['raw_cc'] == 50
    assert jsf['raw_wv'] == 20
    assert jsf['raw_bg'] == 100
    assert jsf['object'] == 'J105553'
    assert jsf['qa_state'] == 'Pass'


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
    assert req.text == '{"result": true, "value": true, ' \
                       '"id": "N20180329S0134.fits"}'

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
    assert req.text == '{"result": true, "value": true, ' \
                       '"id": "GN-2018A-FT-103-13-003"}'

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
    assert req.text == '{"result": true, "value": true, ' \
                       '"id": "GN-2018A-FT-103-13-003"}'

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
    assert req.text == '{"result": true, "value": true, ' \
                       '"id": "N20180329S0134.fits"}'
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
    assert req.status_code == http.HTTPStatus.BAD_REQUEST
    assert req.text == '{"result": false, ' \
                       '"value": "No filename or datalabel given"}'
