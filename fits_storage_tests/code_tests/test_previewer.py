import os
from logging import getLogger

from fits_storage.core.hashes import md5sum
from fits_storage.server.previewer import Previewer
from fits_storage.core.orm.header import Header

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_niri(tmp_path):
    get_test_config()

    data_file = 'N20180329S0134.fits'
    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)

    p = Previewer(diskfile, None, logger=getLogger(), previewpath=tmp_path,
                  using_s3=False, header=header)

    assert p.filename == 'N20180329S0134.jpg'
    assert p.spectrum is False

    assert p.make_preview_file() is True
    assert os.path.exists(p.fpfn) is True
    assert os.path.getsize(p.fpfn) > 10000

    # Not sure how robust this will be - ie jpg compression changes etc.
    # If this fails, check the image is visually OK then update.
    assert md5sum(p.fpfn) == 'b08a262d183c82ccd8af91c5fc899099'

    diskfile.cleanup()


def test_gmos(tmp_path):
    get_test_config()

    data_file = 'N20191002S0080.fits'
    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)

    p = Previewer(diskfile, None, logger=getLogger(), previewpath=tmp_path,
                  using_s3=False, header=header)

    assert p.filename == 'N20191002S0080.jpg'
    assert p.spectrum is False

    assert p.make_preview_file() is True
    assert os.path.exists(p.fpfn) is True
    assert os.path.getsize(p.fpfn) > 10000

    # Not sure how robust this will be - ie jpg compression changes etc.
    # If this fails, check the image is visually OK then update.
    assert md5sum(p.fpfn) == '643b0650c49fa5b2062b92de57fa8e49'

    diskfile.cleanup()
