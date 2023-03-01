import os.path
import hashlib

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File

from fits_storage.core.tests.helpers import fetch_file

from astrodata import AstroData


def test_diskfile(tmp_path):
    storage_root = os.path.join(tmp_path, "storage_root")
    z_staging_dir = os.path.join(tmp_path, "z_staging")
    s3_staging_dir = os.path.join(tmp_path, "s3_staging")

    os.mkdir(storage_root)
    os.mkdir(z_staging_dir)
    os.mkdir(s3_staging_dir)

    test_file = 'N20200127S0023.fits.bz2'
    fetch_file(test_file, storage_root)

    fileobj = File(test_file)
    diskfile = DiskFile(fileobj, test_file, '',
                        storage_root=storage_root,
                        z_staging_dir=z_staging_dir,
                        s3_staging_dir=s3_staging_dir)

    assert diskfile.filename == test_file
    assert diskfile.path == ''
    assert diskfile.present is True
    assert diskfile.canonical is True
    assert diskfile.file_md5 == '8c97a932a9bd13faa15fe3ff5f421f75'
    assert diskfile.file_size == 968412
    assert diskfile.compressed is True
    assert diskfile.data_md5 == 'e781568aff61e671dce3e4ca38cd1323'
    assert diskfile.data_size == 4213440

    assert diskfile.fullpath is not None
    assert diskfile.fullpath.endswith(test_file) is True
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

    # assert diskfile.ad_object is None
    # We close it, but don't set it to None, and there's no reason to.
    # Should check that it's closed though.

    assert os.path.exists(old_cache_file) is False
    assert diskfile.uncompressed_cache_file is None
