import os.path
import types
from astropy.io import fits

from fits_storage.logger import DummyLogger

from fits_storage.scripts.modify_fitsheader import *

def test_modify_fitsheader(tmp_path):
    fitsfn = os.path.join(tmp_path, 'testfile.fits')

    # Create a minimal fits file, consisting a PHU and two HDUs
    phu = fits.PrimaryHDU()
    h1 = fits.ImageHDU()
    h2 = fits.ImageHDU()
    hdulist = fits.HDUList([phu, h1, h2])

    # And write it to a file
    hdulist.writeto(fitsfn)

    # Sanity check initial state
    assert _compare(hdulist, fitsfn)

    # Some housekeeping items
    options = types.SimpleNamespace()
    options.dryrun = False
    options.backup = False
    logger = DummyLogger(print=True)

    # Create and apply an action dicts to the file. Manually update the hdulist
    # with the same change, and check they still match... Rinse and repeat
    # through the different action types

    print('Adding header to PHU')
    action = {'action': 'ADD', 'keyword': 'FOO', 'new_value': 'Bar',
                  'comment': 'FooBar'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[0].header['FOO'] = ('Bar', 'FooBar')
    assert _compare(hdulist, fitsfn)

    print('Set header in PHU')
    action = {'action': 'SET', 'keyword': 'FOO', 'new_value': 'BARF'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[0].header['FOO'] = ('BARF')
    assert _compare(hdulist, fitsfn)

    print('Accepted Update header in PHU')
    action = {'action': 'UPDATE', 'keyword': 'FOO',
              'old_value': 'BARF', 'new_value': 'Wibble'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[0].header['FOO'] = ('Wibble')
    assert _compare(hdulist, fitsfn)

    print('Reject Update header in PHU')
    action = {'action': 'UPDATE', 'keyword': 'FOO',
              'old_value': 'BARF', 'new_value': 'Wobble'}
    modify_fitsfile(fitsfn, [action], options, logger)
    assert _compare(hdulist, fitsfn)

    print('Adding another header to PHU')
    action = {'action': 'ADD', 'keyword': 'BAR', 'new_value': 'Bar',
                  'comment': 'FooBar', 'hdu': 'PHU'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[0].header['BAR'] = ('Bar', 'FooBar')
    assert _compare(hdulist, fitsfn)

    print('Adding header to HDU 2')
    action = {'action': 'ADD', 'keyword': 'FOO', 'hdu': 2,
              'new_value': 'Bar', 'comment': 'FooBar'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[2].header['FOO'] = ('Bar', 'FooBar')
    assert _compare(hdulist, fitsfn)

    print('Set header in HDU 2')
    action = {'action': 'SET', 'keyword': 'FOO', 'new_value': 'BARF', 'hdu': 2}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[2].header['FOO'] = ('BARF')
    assert _compare(hdulist, fitsfn)

    print('Accepted Update header in HDU 2')
    action = {'action': 'UPDATE', 'keyword': 'FOO', 'hdu': 2,
              'old_value': 'BARF', 'new_value': 'Wibble'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[2].header['FOO'] = ('Wibble')
    assert _compare(hdulist, fitsfn)

    print('Reject Update header in HDU 2')
    action = {'action': 'UPDATE', 'keyword': 'FOO', 'hdu': 2,
              'old_value': 'BARF', 'new_value': 'Wobble'}
    modify_fitsfile(fitsfn, [action], options, logger)
    assert _compare(hdulist, fitsfn)

    print('Adding another header to all HDUs')
    action = {'action': 'ADD', 'keyword': 'FOOBAR', 'new_value': 'Foo',
                  'comment': 'FooBar', 'hdu': 'ALL'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[0].header['FOOBAR'] = ('Foo', 'FooBar')
    hdulist[1].header['FOOBAR'] = ('Foo', 'FooBar')
    hdulist[2].header['FOOBAR'] = ('Foo', 'FooBar')
    assert _compare(hdulist, fitsfn)

    print('Set that header in HDU 1 to a new value')
    action = {'action': 'SET', 'keyword': 'FOOBAR', 'new_value': 'FooBar',
              'comment': 'FooBar', 'hdu': 1}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[1].header['FOOBAR'] = ('FooBar', 'FooBar')
    assert _compare(hdulist, fitsfn)

    print('Conditionally modify that header in all HDUs, ie only HDU 1')
    action = {'action': 'UPDATE', 'keyword': 'FOOBAR',
              'old_value': 'FooBar', 'new_value': 'Only',
              'comment': 'FooBar', 'hdu': 'ALL'}
    modify_fitsfile(fitsfn, [action], options, logger)
    hdulist[1].header['FOOBAR'] = ('Only', 'FooBar')
    assert _compare(hdulist, fitsfn)


def _compare(hdulist, fitsfile):
    # Compare the headers in an hdulist and a fitsfile on disk
    with fits.open(fitsfile, mode='readonly') as filehdulist:

        if len(hdulist) != len(filehdulist):
            print('HDUlist lengths differ')
            return False

        for i in range(len(hdulist)):
            h1 = hdulist[i].header
            h2 = filehdulist[i].header

            diff = fits.HeaderDiff(h1, h2)
            if not diff.identical:
                print('HDU %d headers differ:' % i)
                print(diff.report())
                return False

        return True
