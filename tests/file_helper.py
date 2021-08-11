#! python


import os
from datetime import datetime

import fits_storage
import gemini_obs_db
from fits_storage import fits_storage_config
from gemini_obs_db import db_config

for path in (fits_storage_config.storage_root,
             os.path.join(fits_storage_config.storage_root,
                          fits_storage_config.preview_path)):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


def ensure_file(filename, path=None):
    import requests
    import bz2

    # Thinking of dropping the arg, fsc storage root is already set properly
    path = None

    if path is None:
        db_config.storage_root = '/tmp/jenkins_pytest/dataflow'
        fits_storage_config.storage_root = '/tmp/jenkins_pytest/dataflow'
        path = fits_storage_config.storage_root

    if os.path.isfile(os.path.join(path, filename)):
        return

    # Make sure the folder exists.  On Jenkins, it can be transient
    os.makedirs(path, exist_ok=True)

    getfile = filename
    if getfile.endswith(".bz2"):
        getfile = getfile[:-4]
    url = 'https://archive.gemini.edu/file/%s' % getfile
    r = requests.get(url, allow_redirects=True)
    if r.status_code == 200:
        diskfile = os.path.join(path, filename)
        if diskfile.endswith(".bz2"):
            bz2.BZ2File(diskfile, 'w').write(r.content)
        else:
            open(diskfile, 'wb').write(r.content)


def mock_get_file_size(path):
    return 0


def mock_get_file_md5(path):
    return ''


def mock_get_lastmod(path):
    return datetime.now()


def mock_populate_fits(hdr, df, log):
    pass


def mock_populate(ftxthdr, df):
    pass


def setup_mock_file_stuff(monkeypatch):
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_file_size', mock_get_file_size)
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_file_md5', mock_get_file_md5)
    monkeypatch.setattr(gemini_obs_db.orm.diskfile.DiskFile, 'get_lastmod', mock_get_lastmod)
    monkeypatch.setattr(gemini_obs_db.orm.header.Header, 'populate_fits', mock_populate_fits)
    monkeypatch.setattr(fits_storage.orm.fulltextheader.FullTextHeader, 'populate', mock_populate)

