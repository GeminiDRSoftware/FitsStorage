# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.

import http

from fits_storage_tests.testserver_tests.helpers import _uploadfile,\
    _waitforingest


def test_upload(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    resp = _uploadfile(tmp_path, filename)

    assert resp.status_code == http.HTTPStatus.OK
    assert resp.headers['content-type'] == 'application/json'
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
