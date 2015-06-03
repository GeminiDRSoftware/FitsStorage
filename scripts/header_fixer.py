#!/usr/bin/env python

"""Header Fixer

Fixes a FITS header by replacing each instance of <old-value> in <keyword> with <new-value>

Usage:
  header_fixer [-inSj] [-s SRCDIR] [-d DESTDIR] <keyword> <old-value> <new-value> (-f FILELIST | <filename>...)
  header_fixer [-inSj] [-s SRCDIR] [-d DESTDIR] -k SUBSTLIST (-f FILELIST | <filename>...)
  header_fixer -h | --help
  header_fixer --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -d DESTDIR    Directory to write the modified file into. If none is specified, the
                script will create a temporary one and print the path to it.
  -f FILELIST   FILELIST is a file with paths to images, one per line. -s/-S apply to
                the file paths as usual.
  -i            Case insentive match for each <old-value>.
  -j            BZip2-ed output files.
  -k SUBSTLIST  A CSV file with each row describing a <keyword>, <old-value> and <new-value>
  -n            Don't validate the output headers.
  -s SRCDIR     Source directory for FITS files that are not specified with an
                absolute path [default: /net/wikiwiki/dataflow].
  -S            Don't prepend SRCDIR to the file names.
"""

from __future__ import print_function

import functools
import os
import pyfits as pf
import sys
from csv import reader
from bz2 import BZ2File
from collections import namedtuple
from docopt import docopt
from pyfits.verify import VerifyError
from tempfile import mkdtemp

from utils.fits_validator import RuleStack, Environment

args = docopt(__doc__, version='Header Fixer 1.0')

Match = namedtuple('Match', ('key', 'old', 'new'))

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

def needs_change(header, kw, ov, nv):
    try:
        return header[kw] == ov and header[kw] != nv
    except KeyError:
        return False

def change_set(header):
    return ((match.key, match.new) for match in matches if needs_change(header, *match))

def normalized_fn(fn):
    return os.path.basename(fn if not fn.endswith('.bz2') else fn[:-4])

def open_image(path):
    if path.endswith('.bz2'):
        return BZ2File(path)

    return open(path)

def output_file(path):
    if bzipoutput:
        return BZ2File(path + '.bz2', 'w')

    return open(path, 'w')

# Main program
conv_func  = (str.upper if args['-i'] else lambda x: x)
try:
    if args['-k']:
        matches = tuple(Match(*vals) for vals in reader(open(args['-k'])))
    else:
        matches = (Match(args['<keyword>'], args['<old-value>'], args['<new-value>']),)
except IOError as e:
    print(e)
    sys.exit(1)
source_dir = args['-s']
validate   = not args['-i']
pathfn     = ((lambda x: x) if args['-S'] else functools.partial(os.path.join, source_dir))
bzipoutput = args['-j']

try:
    filelist = (args['<filename>'] or (x.strip() for x in open(args['-f'])))
except IOError as e:
    print(e)
    sys.exit(1)

dd = args['-d']
if dd is not None:
    dest_dir = dd
else:
    dest_dir = mkdtemp()
    print('The new files will be written into: {0}'.format(dest_dir))

if validate:
    validator = Tester()

for fn, path in ((normalized_fn(x), pathfn(x)) for x in filelist):
    try:
        fits = pf.open(open_image(path), do_not_scale_image_data = True)
        fits.verify('exception')
        change_sets = filter(lambda x: x[1], [(h.header, change_set(h.header)) for h in fits])
        if not change_sets:
            print("Skipping {0}".format(fn))
            continue
        for header, cset in change_sets:
            for kw, nv in cset:
                header[kw] = nv
        if validate and not validator.valid(fits):
            print("The resulting {0} is not valid".format(fn))
            continue
        fits.writeto(output_file(os.path.join(dest_dir, fn)), output_verify='exception')
    except (IOError, VerifyError) as e:
        print(e)
    except VerifyError as e:
        print('{0} >> {1}'.format(fn, e))
