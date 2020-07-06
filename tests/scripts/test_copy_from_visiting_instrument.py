import os
import pathlib

from fits_storage.scripts.copy_from_visiting_instrument import IGRINS


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


def test_get_destination():
    igrins = igrins_init()
    assert(igrins.get_destination('SDCS_20200101_0001.fits') == 'igrins/20200101/SDCS_20200101_0001.fits')


def test_get_dest_path():
    igrins = igrins_init()
    assert(igrins.get_dest_path('SDCS_20200101_0001.fits') == 'igrins/20200101')
