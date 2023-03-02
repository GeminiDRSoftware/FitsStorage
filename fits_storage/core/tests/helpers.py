"""
Testing helpers
"""
import os
import shutil
import requests

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile

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

def make_diskfile(filename, tmpdir):
    """
    A helper to build a diskfile object as many tests of the orm classes
    require one. This function will set up directories inside the tmpdir,
    will fetch the file, and will instantiate File and DiskFile objects.

    Parameters
    ----------
    filename : str
        Filename of the file
    tmpdir : str or Path
        generally the tmp_dir from pytest. Inside this directory, we will
        create storage_root, z_staging_dir, and s3_staging_dir directories
        that are used in fetching and uncompressing the file.

    Returns
    -------
    a diskfile object for the file.
    """
    storage_root = os.path.join(tmpdir, "storage_root")
    z_staging_dir = os.path.join(tmpdir, "z_staging")
    s3_staging_dir = os.path.join(tmpdir, "s3_staging")

    os.mkdir(storage_root)
    os.mkdir(z_staging_dir)
    os.mkdir(s3_staging_dir)

    fetch_file(filename, storage_root)

    fileobj = File(filename)
    diskfile = DiskFile(fileobj, filename, '',
                        storage_root=storage_root,
                        z_staging_dir=z_staging_dir,
                        s3_staging_dir=s3_staging_dir)

    return diskfile