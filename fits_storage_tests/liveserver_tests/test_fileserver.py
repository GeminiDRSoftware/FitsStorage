import requests
import http
import bz2
import hashlib
import tarfile
import io

from fits_storage_tests.liveserver_tests.helpers import getserver, get_fileinfo


def test_file():
    filename = 'N20180329S0134.fits.bz2'
    server = getserver()

    # Get the metadata for what we expect
    fileinfo = get_fileinfo(filename)

    # Download the file
    url = f"{server}/file/{filename}"
    req = requests.get(url, timeout=10)

    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/fits'

    file_downloaded = req.content
    assert len(file_downloaded) == fileinfo['file_size']
    assert hashlib.md5(file_downloaded).hexdigest() == fileinfo['file_md5']

    fits_data = bz2.decompress(file_downloaded)
    assert len(fits_data) == fileinfo['data_size']
    assert hashlib.md5(fits_data).hexdigest() == fileinfo['data_md5']


def test_download_get():
    filename = 'N20180329S0134.fits.bz2'
    server = getserver()

    # Get the metadata for the payload we expect
    fileinfo = get_fileinfo(filename)

    # Download the tar file
    url = f"{server}/download/{filename}"
    req = requests.get(url, timeout=10)

    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/tar'

    f = io.BytesIO(req.content)
    assert tarfile.is_tarfile(f)
    f.seek(0)

    tf = tarfile.open(fileobj=f)
    tarmembers = tf.getmembers()
    assert len(tarmembers) == 3

    readme_ti = tf.getmember('README.txt')
    assert readme_ti.size == 451

    md5sums_ti = tf.getmember('md5sums.txt')
    assert md5sums_ti.size == 58

    fits_ti = tf.getmember(filename)
    assert fits_ti.size == fileinfo['file_size']
    bz2data = tf.extractfile(fits_ti).read()

    assert len(bz2data) == fileinfo['file_size']
    assert hashlib.md5(bz2data).hexdigest() == fileinfo['file_md5']

    fitsdata = bz2.decompress(bz2data)
    assert len(fitsdata) == fileinfo['data_size']
    assert hashlib.md5(fitsdata).hexdigest() == fileinfo['data_md5']


def test_download_post():
    filename = 'N20180329S0134.fits.bz2'
    server = getserver()

    # Get the metadata for the payload we expect
    fileinfo = get_fileinfo(filename)

    # Download the tar file
    url = f"{server}/download"
    payload = {'files': filename[:-4]}  # strip the .bz2
    req = requests.post(url, data=payload, timeout=10)

    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'application/tar'

    f = io.BytesIO(req.content)
    assert tarfile.is_tarfile(f)
    f.seek(0)

    tf = tarfile.open(fileobj=f)
    tarmembers = tf.getmembers()
    assert len(tarmembers) == 3

    readme_ti = tf.getmember('README.txt')
    assert readme_ti.size == 455

    md5sums_ti = tf.getmember('md5sums.txt')
    assert md5sums_ti.size == 58

    fits_ti = tf.getmember(filename)
    assert fits_ti.size == fileinfo['file_size']
    bz2data = tf.extractfile(fits_ti).read()

    assert len(bz2data) == fileinfo['file_size']
    assert hashlib.md5(bz2data).hexdigest() == fileinfo['file_md5']

    fitsdata = bz2.decompress(bz2data)
    assert len(fitsdata) == fileinfo['data_size']
    assert hashlib.md5(fitsdata).hexdigest() == fileinfo['data_md5']
