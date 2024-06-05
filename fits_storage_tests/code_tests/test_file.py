from fits_storage_tests.code_tests.helpers import get_test_config

from fits_storage.core.orm.diskfile import File


def test_fits():
    get_test_config()

    f = File('filename.fits')

    assert f.name == 'filename.fits'

def test_bz2():
    get_test_config()

    f = File('filename.fits.bz2')

    assert f.name == 'filename.fits'