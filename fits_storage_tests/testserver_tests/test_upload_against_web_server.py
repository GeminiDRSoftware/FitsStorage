# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it. The server needs to have
# service_queue tasks running for ingest and fileops.

import http
import requests
import hashlib
import tarfile
import io

from fits_storage_tests.testserver_tests.helpers import _uploadfile,\
    _waitforingest, getserver


def test_upload_download(tmp_path):
    filename = 'N20180329S0134.fits.bz2'
    resp = _uploadfile(tmp_path, filename)

    assert resp.status_code == http.HTTPStatus.OK
    assert resp.headers['content-type'] == 'application/json'
    assert resp.text == '[{"filename": "N20180329S0134.fits.bz2", ' \
                        '"path": "", ' \
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

    # Now check we can download it via '/file'
    url = getserver() + f"/file/{filename}"
    r = requests.get(url)
    assert r.status_code == http.HTTPStatus.OK
    assert len(r.content) == 1059693
    m = hashlib.md5()
    m.update(r.content)
    assert m.hexdigest() == "1c1c2eb66af5a49218ea95a53b2b9f78"

    # Now check we can download it as a .fits file (ie on the fly decompression) via /file
    fitsfilename = filename.removesuffix(".bz2")
    r = requests.get(url)
    assert r.status_code == http.HTTPStatus.OK
    assert len(r.content) == 4213440
    m = hashlib.md5()
    m.update(r.content)
    assert m.hexdigest() == "6a9688a89307afa7776bd23ea4ccae3f"

    # Now check we can download it via '/download'
    url = getserver() + f"/download/{filename}"
    r = requests.get(url)
    assert r.status_code == http.HTTPStatus.OK
    assert len(r.content) > 1059693

    flo = io.BytesIO(r.content)
    assert tarfile.is_tarfile(flo)
    flo.seek(0)

    tf = tarfile.open(fileobj=flo)
    # Check for README.txt. Will raise KeyError if not present
    ti = tf.getmember('README.txt')
    assert ti.name == 'README.txt'
    # Check for md5sums file. Will raise KeyError if not present
    ti = tf.getmember('md5sums.txt')
    assert ti.name == 'md5sums.txt'
    # Check for dat file. Will raise KeyError if not present
    ti = tf.getmember(filename)
    assert ti.name == filename

    # Check the md5sums.txt is correct
    md5_flo = tf.extractfile('md5sums.txt')
    md5_data = md5_flo.read()
    assert md5_data == b'1c1c2eb66af5a49218ea95a53b2b9f78  N20180329S0134.fits.bz2\n'
    md5_flo.close()

    # Check the data is correct
    data_flo = tf.extractfile(filename)
    data = data_flo.read()
    assert len(data) == 1059693
    m = hashlib.md5()
    m.update(data)
    assert m.hexdigest() == "1c1c2eb66af5a49218ea95a53b2b9f78"
    data_flo.close()
