import os
import re

from fits_storage.scripts.copy_from_visiting_instrument import AlopekeZorroABC


class MockAlopekeZorro(AlopekeZorroABC):
    def __init__(self):
        super().__init__('alopeke', "/tmp/taztest/from", True, storage_root="/tmp/taztest/to")
        self._filename_re = re.compile(r'N\d{8}A\d{4}[br].fits.bz2')
        self._filename_re = re.compile(r'S20200316Z\d{4}[br].fits.bz2')


def test_target_found():
    from pathlib import Path
    Path("/tmp/taztest/from").mkdir(parents=True, exist_ok=True)
    Path("/tmp/taztest/to").mkdir(parents=True, exist_ok=True)
    if os.access("/tmp/taztest/to/foo.fits", os.F_OK):
        os.unlink("/tmp/taztest/to/foo.fits")
    if os.access("/tmp/taztest/to/foo.fits.bz2", os.F_OK):
        os.unlink("/tmp/taztest/to/foo.fits.bz2")

    taz = MockAlopekeZorro()

    # file does not exist
    assert(not taz.target_found("/tmp/taztest/to/foo.fits"))
    # try again bz2
    assert(not taz.target_found("/tmp/taztest/to/foo.fits.bz2"))
    # try again, .fits matching .fits
    open("/tmp/taztest/to/foo.fits", 'a').close()
    # file does exist
    assert(taz.target_found("/tmp/taztest/to/foo.fits"))
    # try again bz2
    assert(taz.target_found("/tmp/taztest/to/foo.fits.bz2"))
    # remove and make a bz2
    os.unlink("/tmp/taztest/to/foo.fits")
    open("/tmp/taztest/to/foo.fits.bz2", 'a').close()
    # file does exist
    assert(taz.target_found("/tmp/taztest/to/foo.fits"))
    # try again bz2
    assert(taz.target_found("/tmp/taztest/to/foo.fits.bz2"))
    os.unlink("/tmp/taztest/to/foo.fits.bz2")
