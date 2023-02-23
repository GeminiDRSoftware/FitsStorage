import pathlib

import fits_storage.fits_storage_config as fsc


def test_verify():
    # point to scratch area for 'dhs'
    fsc.dhs_perm = '/tmp/test_copy_from_dhs'
    # disable email
    fsc.smtp_server = None

    pathlib.Path('/tmp/test_copy_from_dhs').mkdir(parents=True, exist_ok=True)

    dummy_file = '/tmp/test_copy_from_dhs/empty.fits'

    from fits_storage.scripts.copy_from_dhs import validate, _seen_validation_failures

    f = open(dummy_file, "w+")
    f.close()

    check = validate(dummy_file)
    assert(not check)
    assert(_seen_validation_failures[dummy_file] == 1)
    check = validate(dummy_file)
    assert(not check)
    assert(_seen_validation_failures[dummy_file] == 2)
    check = validate(dummy_file)
    assert(not check)
    assert(_seen_validation_failures[dummy_file] == 3)
    check = validate(dummy_file)
    assert(not check)
    assert(_seen_validation_failures[dummy_file] == 4)
