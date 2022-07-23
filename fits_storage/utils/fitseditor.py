import bz2

import astropy.io.fits as pf
import os

from fits_storage.fits_storage_config import z_staging_area


def compare_cards(path, card_dict, ext=0):
    header = pf.getheader(path, ext=0)
    return [(kw in header and header[kw] == value) for kw, value in list(card_dict.items())]


def all_cards_exist(path, card_dict):
    header = pf.getheader(path, ext=0)
    return all((k in header) for k in card_dict)


def get_card(path, keyword, ext=0, skip_missing=True):
    try:
        return pf.getval(path, keyword, ext=ext, do_not_scale_image_data=True)
    except KeyError:
        if not skip_missing:
            raise
        return None


def modify_card(path, keyword, value, ext=0):
    return pf.setval(path, keyword, value=value, ext=ext, do_not_scale_image_data=True)


def modify_multiple_cards(path, card_dict, ext=0):
    uncompressed_cache_file = None
    if path.endswith('.bz2'):
        nonzfilename = path[:-4]
        uncompressed_cache_file = os.path.join(z_staging_area, nonzfilename)
        if os.path.exists(uncompressed_cache_file):
            os.unlink(uncompressed_cache_file)
        with bz2.BZ2File(path, mode='rb') as in_file, open(uncompressed_cache_file, 'wb') as out_file:
            out_file.write(in_file.read())
        workpath = uncompressed_cache_file
    else:
        workpath = path
    with pf.open(workpath, mode='update', do_not_scale_image_data=True) as fitsfile:
        header = fitsfile[ext].header
        header.update(card_dict)
        fitsfile.flush(output_verify="ignore")
    if uncompressed_cache_file is not None:
        os.system('cat %s | bzip2 -sc > %s' % (uncompressed_cache_file, '%s.bz2' % path))
        os.unlink(uncompressed_cache_file)
