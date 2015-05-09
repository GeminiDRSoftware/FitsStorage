#!/usr/bin/env python

"""Header Fixer

Fixes a FITS header by replacing each instance of OV in KW with NV

Usage:
  header_fixer [-inSZ] [-s SRCDIR] [-d DESTDIR]                (-r KW OV NV)... <filename>...
  header_fixer [-inSZ] [-s SRCDIR] [-d DESTDIR] [-f FILELIST ] (-r KW OV NV)...
  header_fixer -h | --help
  header_fixer --version

Options:
  -h --help      Show this screen.
  --version      Show version.
  -d DESTDIR     Directory to write the modified file into. If none is specified, the
                 script will create a temporary one and print the path to it.
  -f FILELIST    FILELIST is a file with paths to images, one per line. -s/-S apply to
                 the file paths as usual.
  -i             Case insentive match for each OV
  -n             Don't validate the output headers
  -r KW OV NV    Replace OV with NV on keyword KW
  -s SRCDIR      Source directory for FITS files [default: /net/wikiwiki/dataflow].
  -S             Don't prepend SRCDIR to the file names
  -Z             BZip2-ed output files
"""

from __future__ import print_function

import functools
import os
import pyfits as pf
import sys
from docopt import docopt
from pyfits.verify import VerifyError
from tempfile import mkdtemp

from utils.fits_validator import RuleStack, Environment

arguments = docopt(__doc__, version='Header Fixer 1.0')

class Tester(object):
    def __init__(self):
        self.rs = RuleStack()
        self.rs.initialize('fits')

    def valid(self, fits):
        env = Environment()
        env.features = set()
        res = []
        mess = []
        for n, hdu in enumerate(fits):
            env.numHdu = n
            t = self.rs.test(hdu.header, env)
            res.append(t[0])
            mess.extend(t[1])
        return all(res), mess

def should_modify(header):
    return any((header[kw] == ov and header[kw] != nv) for (kw, ov, nv) in matches if kw in header)

# Main program
conv_func  = (str.upper if arguments['-i'] else lambda x: x)
matches    = tuple((kw, conv_func(ov), nv) for (kw, ov, nv) in zip(arguments['-r'], arguments['OV'], arguments['NV']))
source_dir = arguments['-s']
validate   = not arguments['-i']
pathfn     = ((lambda x: x) if arguments['-S'] else functools.partial(os.path.join, source_dir))

try:
    filelist = (arguments['<filename>'] or open(arguments['-f']))
except IOError as e:
    print(e)
    sys.exit(1)

dd = arguments['-d']
if dd is not None:
    dest_dir = dd
else:
    dest_dir = mkdtemp()
    print('The new files will be written into: {0}'.format(dest_dir))

if validate:
    validator = Tester()

for fn, path in ((os.path.basename(x), pathfn(x)) for x in filelist):
    try:
        fits = pf.open(path, do_not_scale_image_data = True)
        fits.verify('exception')
        to_modify = [x.header for x in fits if should_modify(x.header)]
        if not to_modify:
            print("Skipping {0}".format(fn))
            continue
        for header in to_modify:
            for kw, _, nv in matches:
                if kw not in header:
                    continue
                header[kw] = nv
        if validate and not validator.valid(fits):
            print("The resulting {0} is not valid".format(fn))
            continue
        fits.writeto(open(os.path.join(dest_dir, fn), 'w'), output_verify='exception')
    except (IOError, VerifyError) as e:
        print(e)
    except VerifyError as e:
        print('{0} >> {1}'.format(fn, e))
