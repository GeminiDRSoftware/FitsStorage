#! python


import os

from fits_storage import fits_storage_config


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

    if path is None:
        path = fits_storage_config.storage_root

    if os.path.isfile(os.path.join(path, filename)):
        return

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
