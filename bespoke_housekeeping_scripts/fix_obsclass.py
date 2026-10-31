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
        print(f"File {fn} - {mode}")
        hdulist = fits.open(fn, mode=mode, lazy_load_hdus=True,
                            do_not_scale_image_data=True)
        phu = hdulist[0].header
        if 'OBSCLASS' in phu:
            print(f"  - OBSCLASS already defined: {phu['OBSCLASS']}. Skipping")
            hdulist.close()
            continue
        obstype = phu.get('OBSTYPE')
        if obstype is None:
            print(f"  - OBSTYPE is None. Skipping")
            hdulist.close()
            continue
        if obstype in ('BIAS', 'DARK'):
            print(f"  - BIAS / DARK. Setting dayCal")
            phu['OBSCLASS'] = ("dayCal", 'Fixed missing OBSCLASS in old data')
        elif obstype in ('FLAT', 'ARC'):
            print(f"  - FLAT / ARC. Setting partnerCal")
            phu['OBSCLASS'] = ("partnerCal", 'Fixed missing OBSCLASS in old data')
        elif obstype == 'OBJECT':
            object = phu.get('OBJECT')
            if object and object.lower in ('twilight', 'zenith'):
                print(f"  - Twilight. Setting dayCal")
                phu['OBSCLASS'] = ("dayCal", 'Fixed missing OBSCLASS in old data')
            else:
                print(f"  - Assuming science. Setting dayCal")
                phu['OBSCLASS'] = ("science", 'Fixed missing OBSCLASS in old data')
        hdulist.close()


if __name__ == "__main__":
    main()