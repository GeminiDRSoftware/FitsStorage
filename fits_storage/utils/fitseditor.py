import astropy.io.fits as pf
import os

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
    with pf.open(path, mode='update', do_not_scale_image_data=True) as fitsfile:
        header = fitsfile[ext].header
        header.update(card_dict)
        fitsfile.flush(output_verify="ignore")
