"""
Testing helpers
"""
import os
import shutil
import requests


def fetch_file(filename, dest_dir):
    """
    Fetch a file from the test data directory if it exists there,
    or from the archive if not, into dest_dir.

    The test data directory is defined by the environment variable
    FITS_STORAGE_TEST_DATA
    """

    destination = os.path.join(dest_dir, filename)
    srcdir = os.environ.get('FITS_STORAGE_TEST_DATA')
    if srcdir:
        source = os.path.join(srcdir, filename)
        if os.path.exists(source):
            print(f"Copying {source} to {destination}")
            shutil.copyfile(source, destination)
            return
    url = 'https://archive.gemini.edu/file/%s' % filename
    print(f"Downloading {url} to {destination}")
    r = requests.get(url, allow_redirects=True)
    if r.status_code == 200:
        with open(destination, 'wb') as f:
            f.write(r.content)
