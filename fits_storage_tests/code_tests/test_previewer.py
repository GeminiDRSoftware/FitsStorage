import os
from logging import getLogger
import shutil

from fits_storage.core.hashes import md5sum
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

    # Not sure how robust this will be - ie jpg compression changes etc.
    # If this fails, check the image is visually OK then update.
    assert md5sum(p.fpfn) == 'e3a477c5430e9468d749f6136d9f07d1'

    diskfile.cleanup()


def test_gmos(tmp_path):
    data_file = 'N20191002S0080.fits'
    diskfile = make_diskfile(data_file, tmp_path)

    p = Previewer(diskfile, None, logger=getLogger(), path=tmp_path,
                  using_s3=False)

    assert p.filename == 'N20191002S0080.jpg'
    assert p.spectrum is False

    assert p.make_preview_file() is True
    assert os.path.exists(p.fpfn) is True
    assert os.path.getsize(p.fpfn) > 10000

    # Not sure how robust this will be - ie jpg compression changes etc.
    # If this fails, check the image is visually OK then update.
    assert md5sum(p.fpfn) == 'b766dd58982d20d4ef0f156bf59c2fce'

    diskfile.cleanup()


