import os
import os.path

import astrodata

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile

from fits_storage.server.fitseditor import FitsEditor
from fits_storage.logger import DummyLogger

def get_from_file(fpfn, headers=[]):
    get_test_config()

    ad = astrodata.open(fpfn)
    d = {}
    d['qa_state'] = ad.qa_state()
    d['rawbg'] = ad.raw_bg()
    d['rawcc'] = ad.raw_cc()
    d['rawiq'] = ad.raw_iq()
    d['rawwv'] = ad.raw_wv()
    d['release'] = ad.phu.get('RELEASE')
    for h in headers:
        d[h] = ad.phu.get(h)
    return d


def test_fitseditor(tmp_path):
    get_test_config()

    diskfile = make_diskfile('N20200127S0023.fits.bz2', tmp_path)
    df_fp = diskfile.fullpath

    orig_lastmod = diskfile.get_file_lastmod()

    # Get values from fits file and verify initial values
    ff = get_from_file(df_fp)
    assert ff['qa_state'] == 'Usable'
    assert ff['rawbg'] == 20
    assert ff['rawcc'] == 50
    assert ff['rawiq'] == 70
    assert ff['rawwv'] == 20
    assert ff['release'] == '2020-07-27'

    fe = FitsEditor(filename='N20200127S0023.fits', do_setup=False,
                    logger=DummyLogger(print=True))
    fe.diskfile = diskfile
    fe._get_localfile()
    fe._get_hdulist()

    # Since the file given is a bz2 file, there should be an uncompressed
    # cache file at this point. Remember the name for later reference
    ucf = diskfile.uncompressed_cache_file
    assert os.path.isfile(ucf)

    assert fe.error is False
    # Update some headers
    assert fe.set_qa_state('Pass') is True
    assert fe.set_rawsite('bg80') is True
    assert fe.set_rawsite('cc70') is True
    assert fe.set_rawsite('iq85') is True
    assert fe.set_rawsite('wvany') is True
    assert fe.set_release('2025-01-02') is True

    assert fe.set_header('SSA', 'Mickey Mouse', reject_new=True) is True

    assert fe.set_header('FELINE', 'Sleepy', reject_new=False) is True

    assert fe.set_header('KANGAROO', 'Jumpy', reject_new=True) is False

    # Close (and write) the file
    fe.close()

    # The uncompressed cache file should have been deleted by fe.close()
    assert not os.path.isfile(ucf)

    # There should be nothing in storage_root other than the diskfile
    ld = os.listdir(diskfile.storage_root)
    assert ld == ['N20200127S0023.fits.bz2']

    new_lastmod = diskfile.get_file_lastmod()
    assert new_lastmod > orig_lastmod

    # Verify new values in file
    ff = get_from_file(df_fp, headers=['SSA', 'FELINE', 'KANGAROO'])
    assert ff['qa_state'] == 'Pass'
    assert ff['rawbg'] == 80
    assert ff['rawcc'] == 70
    assert ff['rawiq'] == 85
    assert ff['rawwv'] == 100
    assert ff['release'] == '2025-01-02'
    assert ff['SSA'] == 'Mickey Mouse'
    assert ff['FELINE'] == 'Sleepy'
    assert ff['KANGAROO'] is None
