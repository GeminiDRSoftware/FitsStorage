from fits_storage.core.orm.diskfile import File

def test_fits():
    f = File('filename.fits')

    assert f.name == 'filename.fits'

def test_bz2():
    f = File('filename.fits.bz2')

    assert f.name == 'filename.fits'