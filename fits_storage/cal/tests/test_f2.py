import os

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from fits_storage.cal.orm.f2 import F2
from fits_storage.core.tests.helpers import fetch_file


def test_f2(tmp_path):
    storage_root = os.path.join(tmp_path, "storage_root")
    z_staging_dir = os.path.join(tmp_path, "z_staging")
    s3_staging_dir = os.path.join(tmp_path, "s3_staging")

    os.mkdir(storage_root)
    os.mkdir(z_staging_dir)
    os.mkdir(s3_staging_dir)

    test_file = 'S20181219S0333.fits.bz2'
    fetch_file(test_file, storage_root)
    fileobj = File(test_file)
    diskfile = DiskFile(fileobj, test_file, '',
                        storage_root=storage_root,
                        z_staging_dir=z_staging_dir,
                        s3_staging_dir=s3_staging_dir)
    header = Header(diskfile)
    f2 = F2(header, diskfile.get_ad_object)

    assert (f2.disperser == 'JH_G5801')
    assert (f2.filter_name == 'Open&JH_G0809')
    assert (f2.lyot_stop == 'f/16_G5830')
    assert (f2.read_mode == '1')
    assert (f2.focal_plane_mask == '4pix-slit')
