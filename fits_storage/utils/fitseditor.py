import pyfits as pf
import os

def get_card(path, keyword, ext=0, skip_missing=True):
    try:
        return pf.getval(path, keyword, ext=ext, do_not_scale_image_data=True)
    except KeyError:
        if not skip_missing:
            raise
        return None

def modify_card(path, keyword, value, ext=0):
    return pf.setval(path, keyword, value=value, ext=ext, do_not_scale_image_data=True)
