import os.path
import hashlib

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile

from astrodata import AstroData


def test_diskfile(tmp_path):
    get_test_config()
    diskfile = make_diskfile('N20200127S0023.fits.bz2', tmp_path)

    assert diskfile.filename == 'N20200127S0023.fits.bz2'
    assert diskfile.path == ''
    assert diskfile.present is True
    assert diskfile.canonical is True
    assert diskfile.file_md5 == '8c97a932a9bd13faa15fe3ff5f421f75'
    assert diskfile.file_size == 968412
    assert diskfile.compressed is True
    assert diskfile.data_md5 == 'e781568aff61e671dce3e4ca38cd1323'
    assert diskfile.data_size == 4213440

    assert diskfile.fullpath is not None
    assert diskfile.fullpath.endswith('N20200127S0023.fits.bz2') is True
    assert diskfile.uncompressed_cache_file is not None
    assert diskfile.file_exists() is True
    assert os.path.getsize(diskfile.uncompressed_cache_file) == 4213440

    hashobj = hashlib.md5()
    with open(diskfile.uncompressed_cache_file, 'rb') as f:
        hashobj.update(f.read())
    assert hashobj.hexdigest() == 'e781568aff61e671dce3e4ca38cd1323'

    assert diskfile.get_ad_object is not None
    assert isinstance(diskfile.ad_object, AstroData)

    assert 'NIRI' in diskfile.ad_object.tags

    old_cache_file = diskfile.uncompressed_cache_file
    diskfile.cleanup()

    assert os.path.exists(old_cache_file) is False
    assert diskfile.ad_object is None
    assert diskfile.uncompressed_cache_file is None
