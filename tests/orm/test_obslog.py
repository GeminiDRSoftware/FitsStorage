from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.file import File
from fits_storage.orm.obslog import Obslog
from tests.file_helper import ensure_file
import fits_storage.fits_storage_config as fsc


def test_oblsog(monkeypatch):
    monkeypatch.setattr(fsc, "storage_root", "/tmp")

    data_file = '20200101_GS-2019B-FT-105_obslog.txt'

    ensure_file(data_file, '/tmp')

    f = File(data_file)
    df = DiskFile(f, data_file, "")
    obslog = Obslog(df)

    assert(obslog.program_id == 'GS-2019B-FT-105')
    assert(obslog.__repr__() == "<Obslog('None', 'GS-2019B-FT-105', '2020-01-01')>")
