import datetime

from fits_storage.server.orm.miscfile import *
from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.config import get_config

class FakeDiskFile(object):
    id = 1
    filename = 'filename'


def test_miscfile_orm():
    # Very basic sanity check of ORM object
    fdf = FakeDiskFile()
    mf = MiscFile(fdf, parse_meta=False)

    assert mf.diskfile_id == 1

    mf.description = "foo"
    mf.release = '2000-01-01'

    assert mf.description == 'foo'
    assert mf.release == '2000-01-01'


def test_misfileprefix():
    # If this changes, the existing archive will need manually updating!
    assert MISCFILE_PREFIX == 'miscfile_'


def test_normalize_diskname():
    assert normalize_diskname("foo") == "miscfile_foo"
    assert normalize_diskname("miscfile_bar") == "miscfile_bar"

def test_miscfile(tmp_path):
    # A More complete MiscFile object test.
    make_empty_testing_db_env(tmp_path)
    fsc = get_config()

    mf_filename = "miscfile_test.dat"
    # Create a dummy miscfile in storage_root
    mf_path = os.path.join(fsc.storage_root, mf_filename)
    # Doesn't actually need to be a tar file. We (by definition) never look
    # inside these anyway.
    with open(mf_path, 'w') as f:
        f.write("Hello, World...")

    # Create a diskfile from it:
    f = File(mf_filename)
    df = DiskFile(f, mf_filename, '')

    # Create a miscfile "meta" json file in upload_staging. This is normally
    # created from the web form inputs when you upload one, and the values are
    # stored in S3 metadata fields, then read directly from there if ingesting
    # from S3.

    mf_metapath = os.path.join(fsc.upload_staging_dir,
                               miscfile_meta_path(df.filename))
    testmeta = {'program': 'GN-2000A-Q-1',
                'release': '2001-01-01',
                'filename': mf_filename,
                'description': 'Test miscfile description'}
    with open(mf_metapath, 'w') as f:
        json.dump(testmeta, f)

    # Now instantiate a MiscFile instance
    miscfile = MiscFile(df)

    assert miscfile.release == datetime.datetime(2001, 1, 1, 0, 0)
    assert miscfile.program_id == 'GN-2000A-Q-1'
    assert miscfile.description == 'Test miscfile description'
