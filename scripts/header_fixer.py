#!/usr/bin/env python

"""Header Fixer

Fixes a FITS header by replacing the value of <keyword> with <new_value>.

Usage:
  header_fixer <keyword> <new_value> <filename>... [-i] [-w MATCH ...] [-s SRCDIR] [-d DESTDIR]
  header_fixer -h | --help
  header_fixer --version

Options:
  -h --help   Show this screen.
  --version   Show version.
  -d DESTDIR  Directory to write the modified file into. If none is specified, the
              script will create a temporary one and print the path to it.
  -i          Case insentive comparison for MATCHes (if specified). Meaningful only
              for string values... The change will be skipped if the original value
              is *exactly* the new one, though.
  -s SRCDIR   Source directory for FITS files [default: /net/wikiwiki/dataflow].
  -w MATCH    Only fix the files where the <keyword>'s value matches the specified
              value. Can be passed multiple times. If no match is specified, the
              header will be changed for every file.
"""

from __future__ import print_function

import os
import pyfits as pf
import sys
from docopt import docopt
from tempfile import mkdtemp

arguments = docopt(__doc__, version='Header Fixer 1.0')

# Main program
conv_func  = (str.upper if arguments['-i'] else lambda x: x)
matches    = set(conv_func(x) for x in arguments['-w'])
source_dir = arguments['-s']
keyword    = arguments['<keyword>']
new_value  = arguments['<new_value>']
dd = arguments['-d']
if dd is not None:
    dest_dir = dd
else:
    dest_dir = mkdtemp()
    print('The new files will be written into: {0}'.format(dest_dir))

def should_modify(header):
    try:
        val = header[keyword]
        return val != new_value and (matches and conv_func(val) in matches)
    except KeyError:
        return False

# TODO: Add a checkup to make sure the final data is actually right...

for fn in arguments['<filename>']:
    try:
        fits = pf.open(os.path.join(source_dir, fn), do_not_scale_image_data = True)
        to_modify = [x.header for x in fits if should_modify(x.header)]
        if not to_modify:
            print("Skipping {0}".format(fn))
            continue
        for header in to_modify:
            header[keyword] = new_value
        fits.writeto(open(os.path.join(dest_dir, fn), 'w'), output_verify='exception')
    except IOError as e:
        print(e)
