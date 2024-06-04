"""
Testing helpers
"""
import os
import shutil
import requests

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.db import sessionfactory

from fits_storage.config import get_config

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

    Note, this function does not create or use a database or sqlalchemy
    session, it instantiates and configures some ORM objects but
    never adds them to a session.

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

def make_empty_testing_db_env(tmpdir):
    """
    Make a testing environment consisting an empty in memory sqlite database
    ready for use, and a tmpdir populated with storage_root and staging
    directories, the locations of which are pushed into the fits storage
    config system.

    Parameters
    ----------

    tmpdir : str or Path
        generally the tmp_dir from pytest. Inside this directory, we will
        create empty staging directories.

    Returns
    -------
    Nothing. Relevant values are stored in the fits storage config
    global object
    """

    storage_root = os.path.join(tmpdir, "storage_root")
    z_staging_dir = os.path.join(tmpdir, "z_staging")
    s3_staging_dir = os.path.join(tmpdir, "s3_staging")
    upload_staging_dir = os.path.join(tmpdir, "upload_staging")
    dbfile = os.path.join(tmpdir, "sqlite.db")

    os.mkdir(storage_root)
    os.mkdir(z_staging_dir)
    os.mkdir(s3_staging_dir)
    os.mkdir(upload_staging_dir)

    configstring = f"""
        [DEFAULT]
        storage_root = {storage_root}
        z_staging_dir = {z_staging_dir}
        s3_staging_dir = {s3_staging_dir}
        upload_staging_dir = {upload_staging_dir}
        database_url = sqlite:///{dbfile}
        is_server = True
        export_destinations =
        """
    fsc = get_config(configstring=configstring, builtinonly=True, reload=True)

    # Note - must do this import *after* the call to get_config() above this
    # import references the fsc at import time.
    from fits_storage.db.createtables import create_tables, drop_tables

    session = sessionfactory(reload=True)
    drop_tables(session)
    create_tables(session)
