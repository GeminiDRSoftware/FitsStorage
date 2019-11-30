from fits_storage.fits_verify import fitsverify
import pytest


def test_fitsverify():
    isfits, warnings, errors, report = fitsverify('testdata/N20191008S0482.fits')
    assert(isfits)
    assert(errors == '0')
    print("done")
