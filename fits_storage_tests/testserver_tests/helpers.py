import os


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
