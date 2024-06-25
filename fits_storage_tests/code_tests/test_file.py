from fits_storage_tests.code_tests.helpers import get_test_config
from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.core.orm.diskfile import File

from fits_storage.db import sessionfactory


def test_fits():
    get_test_config()

    f = File('filename.fits')

    assert f.name == 'filename.fits'

def test_bz2():
    get_test_config()

    f = File('filename.fits.bz2')

    assert f.name == 'filename.fits'

def test_file_against_db(tmp_path):
    make_empty_testing_db_env(tmp_path)
    session = sessionfactory()

    f = File('filename.fits')

    # Not in a database session yet, should have no id
    assert f.id is None

    session.add(f)
    session.commit()

    # Now it should have a valid id as the database layer should have assigned
    # one as part of the primary key triggered id sequence. Note the value
    # to compare with later...
    assert f.id is not None
    our_file_id = f.id

    # Now build the database query and verify we can get the file instance back.
    q = session.query(File).filter(File.name=='filename.fits')

    # There should be exactly one result. This will raise an exception otherwise
    g = q.one()

    # and sanity check what we got back
    assert g.name == 'filename.fits'
    assert g.id == our_file_id

