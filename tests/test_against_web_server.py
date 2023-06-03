# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.
# The base address is defined here:

base_url = 'http://localhost:8000/'

import os.path
import http
import requests
import time

from fits_storage.config import get_config

from helpers import fetch_file

def _jsonsummary(filename):
    url = f"{base_url}/jsonsummary/present/{filename}"
    return requests.get(url).json()

def test_upload_update(tmp_path):
    fsc = get_config()
    filename = 'N20180329S0134.fits'
    print('Fetching File')
    fetch_file(filename, tmp_path)

    # upload the file to the server
    print("Uploading File...")
    fpfn = os.path.join(tmp_path, filename)
    url = f"{base_url}/upload_file/{filename}"
    print(f"upload URL is {url}")

    with open(fpfn, mode='rb') as f:
        req = requests.post(url, data=f, timeout=10)
        assert req.status_code == http.HTTPStatus.OK
        assert req.text == '[{"filename": "N20180329S0134.fits", "size": 4213440, "md5": "6a9688a89307afa7776bd23ea4ccae3f"}]'

    # Now we basically poll jsonsummary, waiting for the ingest to complete
    print("Waiting for ingest to complete")
    waiting = True
    timeout = 20
    js = None
    while waiting:
        time.sleep(1)
        timeout -= 1
        assert timeout > 0
        js = _jsonsummary(filename)
        if len(js) and js[0]['data_md5'] == '6a9688a89307afa7776bd23ea4ccae3f':
            waiting = False

    # Sanity check initial values
    jsf = js[0]
    assert jsf['name'] == filename
    assert jsf['data_md5'] == '6a9688a89307afa7776bd23ea4ccae3f'
    assert jsf['release'] == '2018-09-29'
    assert jsf['raw_iq'] == 85
    assert jsf['raw_cc'] == 50
    assert jsf['raw_wv'] == 20
    assert jsf['raw_bg'] == 100
    assert jsf['object'] == 'J105553'
    assert jsf['qa_state'] == 'Pass'

    print("Ingested OK")

    print("Testing old format header update by filename")
    # Create the header_update payload, old format
    msg = '[{"filename": "N20180329S0134.fits", "values": {"qa_state": "Usable"}}]'
    url = f"{base_url}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '{"result": true, "value": true, "id": "N20180329S0134.fits"}'
    print("Waiting for ingest to complete")
    waiting = True
    timeout = 20
    while waiting:
        time.sleep(1)
        timeout -= 1
        assert timeout > 0
        js = _jsonsummary(filename)
        if len(js) and js[0]['data_md5'] == 'd5f5b0c59ac7ba904d98ca1f343254a4':
            waiting = False
    print("Ingested OK")
    jsf = js[0]
    assert jsf['qa_state'] == 'Usable'

    print("Testing old format header update by datalabel")
    # Create the header_update payload, old format
    msg = '[{"data_label": "GN-2018A-FT-103-13-003", "values": {"raw_site": "iqany"}}]'
    url = f"{base_url}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '{"result": true, "value": true, "id": "GN-2018A-FT-103-13-003"}'
    print("Waiting for ingest to complete")
    waiting = True
    timeout = 20
    while waiting:
        time.sleep(1)
        timeout -= 1
        assert timeout > 0
        js = _jsonsummary(filename)
        if len(js) and js[0]['data_md5'] == '65a9baa43115b07d60b6d1eae73de0a5':
            waiting = False
    print("Ingested OK")
    jsf = js[0]
    assert jsf['raw_iq'] == 100

    print("Testing new format header update by datalabel")
    # Create the header_update payload, old format
    msg = '{"request": [{"data_label": "GN-2018A-FT-103-13-003", "values": {"release": "2123-04-05"}, "reject_new": true}], "batch": false}'
    url = f"{base_url}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '{"result": true, "value": true, "id": "GN-2018A-FT-103-13-003"}'
    print("Waiting for ingest to complete")
    waiting = True
    timeout = 20
    while waiting:
        time.sleep(1)
        timeout -= 1
        assert timeout > 0
        js = _jsonsummary(filename)
        if len(js) and js[0]['data_md5'] == '3c709f93be1bf20f6c59f64aa075b0cf':
            waiting = False
    print("Ingested OK")
    jsf = js[0]
    assert jsf['release'] == '2123-04-05'

    print("Testing new format header update by filename")
    # Create the header_update payload, old format
    msg = '{"request": [{"filename": "N20180329S0134.fits", "values": {"raw_site": "iq70"}, "reject_new": true}], "batch": false}'
    url = f"{base_url}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.OK
    assert req.text == '{"result": true, "value": true, "id": "N20180329S0134.fits"}'
    print("Waiting for ingest to complete")
    waiting = True
    timeout = 20
    while waiting:
        time.sleep(1)
        timeout -= 1
        assert timeout > 0
        js = _jsonsummary(filename)
        if len(js) and js[0]['data_md5'] == '16c3e280a405726ee62ea5ad18f3d03c':
            waiting = False
    print("Ingested OK")
    jsf = js[0]
    assert jsf['raw_iq'] == 70

    print("Testing new format bad request")
    # Create the header_update payload, old format
    msg = '{"request": [{"typo": "N20180329S0134.fits", "values": {"raw_site": "iqany"}, "reject_new": true}], "batch": false}'
    url = f"{base_url}/update_headers"
    req = requests.post(url, data=msg, timeout=10)
    assert req.status_code == http.HTTPStatus.BAD_REQUEST
    assert req.text == '{"result": false, "value": "No filename or datalabel given"}'

