import os
import re
import pathlib

from fits_storage.scripts.copy_from_visiting_instrument import IGRINS
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



def igrins_init(remove_sri=False):
    pathlib.Path('/tmp/test/cfvii/igrins/2020A/20200101').mkdir(parents=True, exist_ok=True)
    pathlib.Path('/tmp/test/cfvii/storage_root').mkdir(parents=True, exist_ok=True)

    igrins = IGRINS(base_path='/tmp/test/cfvii/igrins', storage_root='/tmp/test/cfvii/storage_root')

    if remove_sri:
        if os.path.exists('/tmp/test/cfvii/storage_root/igrins'):
            os.rmdir('/tmp/test/cfvii/storage_root/igrins')
        else:
            igrins.prep()
    return igrins


def test_prep():
    igrins = igrins_init(remove_sri=True)
    igrins.prep()
    assert(os.path.exists('/tmp/test/cfvii/storage_root/igrins'))


def test_get_files():
    igrins = igrins_init()
    f = open('/tmp/test/cfvii/igrins/2020A/20200101/SDCS_20200101_0001.fits', "w+")
    f.close()
    lst = list()
    lst.extend(igrins.get_files())
    assert(lst)
    assert(len(lst) == 1)
    assert(lst[0] == '/tmp/test/cfvii/igrins/2020A/20200101/SDCS_20200101_0001.fits')


def test_get_dest_path():
    igrins = igrins_init()
    assert(igrins.get_dest_path('SDCS_20200101_0001.fits') == 'igrins/20200101')
