#! /usr/bin/env python3

from optparse import OptionParser
import re
import json
import os.path
from astropy.io import fits


def main():
    usage = "usage: %prog [options] filename..."
    parser = OptionParser(usage)
    parser.add_option("--dryrun", action="store_true", dest="dryrun",
                      help="Do not actually modify files, but run through the "
                           "files and report what would actually be done")

    (options, args) = parser.parse_args()

    for fn in os.listdir('.'):
        if not fn.endswith(".fits"):
            print(f"Skipping {fn}")
            continue
        mode = "update"
        if options.dryrun:
            mode="readonly"
        hdulist = fits.open(fn, mode=mode, lazy_load_hdus=True,
                            do_not_scale_image_data=True)
        phu = hdulist[0].header
        if phu.get('DATE') == 'No Value':
            print("{fn}  - Fixing null date")
            phu['DATE'] = (None, 'Corrected invalid No Value')
        hdulist.close()


if __name__ == "__main__":
    main()