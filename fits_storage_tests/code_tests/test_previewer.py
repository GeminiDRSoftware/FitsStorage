import os
from logging import getLogger

from fits_storage.server.previewer import Previewer

from fits_storage_tests.code_tests.helpers import make_diskfile

def test_niri(tmp_path):
    data_file = 'N20180329S0134.fits'
    diskfile = make_diskfile(data_file, tmp_path)

    p = Previewer(diskfile, None, logger=getLogger(), path=tmp_path, using_s3=False)

    assert p.filename == 'N20180329S0134.jpg'
    assert p.spectrum is False

    assert p.make_preview_file() is True
    assert os.path.exists(p.fpfn) is True

    assert os.path.getsize(p.fpfn) > 10000

    diskfile.cleanup()