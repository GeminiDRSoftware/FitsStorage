import os
import requests
import time

from fits_storage_tests.code_tests.helpers import fetch_file


def getserver():
    """
    Returns a server name (in the form https://archive.gemini.edu )
    either using the FITS_STORAGE_TEST_TESTSERVER environment variable,
    or defaulting to localhost:8000 if that is not set.
    """
    server = os.environ.get('FITS_STORAGE_TEST_TESTSERVER')
    if server is None:
        server = 'http://localhost:8000'

    print(f'Using test server: {server}')
    return server


def _jsonsummary(filename):
    # Helper to return json from jsonsummary for a given filename
    url = f"{getserver()}/jsonsummary/present/{filename}"
    return requests.get(url).json()


def _uploadfile(tmp_path, filename):
    # Helper to upload a file to the testserver. This fetches the file and HTTP
    # POSTs it to the server, and returns the requests response directly. It
    # doesn't wait for it to be ingested or check that it did get ingested

    server = getserver()
    print('Fetching File')
    fetch_file(filename, tmp_path)

    # upload the file to the server
    print("Uploading File...")
    fpfn = os.path.join(tmp_path, filename)
    url = f"{server}/upload_file/{filename}"
    print(f"upload URL is {url}")

    with open(fpfn, mode='rb') as f:
        try:
            req = requests.post(url, data=f, timeout=30)
        except Exception:
            print(f"Exception posting to {url}")
            raise
    return req


def _waitforingest(filename, data_md5=None, timeout=None):
    # Helper function that polls the server, waiting for the ingest of filename
    # to complete. If md5 is not given, don't check it. If timeout is not
    # given, default to 20. Return the jsonsummary if the file gets ingested,
    # None if we time out.

    # We poll jsonsummary, waiting for the ingest to complete
    print("Waiting for ingest to complete")
    waiting = True
    timeout = timeout if timeout else 20
    js = None
    while waiting:
        time.sleep(1)
        timeout -= 1
        if timeout == 0:
            return None
        js = _jsonsummary(filename)
        if len(js):
            if data_md5 is None:
                waiting = False
            elif js[0]['data_md5'] == data_md5:
                waiting = False
    return js


def _ensureuploaded(tmp_path, filename, data_md5=None):
    # A shortcut that ensures a file has been uploaded. Useful if that file
    # is used in the test and in other tests. Yes, this is somewhat contrary
    # to the "tests should be independent" mantra. But it's a pragmatic
    # solution here and avoids having a small number of very long tests, or
    # requiring the test execution to be in a certain order, or re-uploading
    # files unconditionally each time.
    # if data_md5 is passed, we ensure the file uploaded has that data_md5.
    # Otherwuse, we just ensure that a file with that filename is present in
    # the test server database.
    # Returns the jsonsummary result on Success, None on Failure
    js = _jsonsummary(filename)
    # We gave a filename, and jsonsummary preselects canonical, so should only
    # be one result
    if js and data_md5 is None:
        # File exists and we don't care about the md5
        return js
    if js and js[0]['data_md5'] == data_md5:
        # File exists with the correct md5
        return js
    # If we get here, either it doesn't exist or it has the wrong md5
    _uploadfile(tmp_path, filename)
    return _waitforingest(filename)
