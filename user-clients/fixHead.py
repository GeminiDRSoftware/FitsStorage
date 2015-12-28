#! /usr/bin/env python

#************************************************************************
#****              G E M I N I  O B S E R V A T O R Y                ****
#************************************************************************
#
#   Script name:        hupdate
#
#   Purpose:
#      Provide a user interface to the Gemini Archive API that allows
#      modification of raw FITS file headers, with validation.
#
#      This script is tailored to SOS needs.
#
#   Date               : 2015-12-09
#
#   Author             : Ricardo Cardenes
#
#   Modification History:
#    2015-12-09, rcardene : First release

"""
FITS Header Fixing Tool v0.1

Usage:

 fixHead [-hdy] [--ext=EXT] [<date>] <filenums> KEYW:VALUE [KEYW:VALUE ...]
 fixHead [-hdy] [--ext=EXT] obsid <observation-id> KEYW:VALUE [KEYW:VALUE ...]

Arguments:

  <date>            Optional, in format YYYYMMDD. If not specified, the current
                    date is assumed.
  <filenums>        A comma-separated list of number ranges. Eg: 10-20,31,35-40
  <observation-id>  A valid Gemini observation ID
  KEYW              A keyword or one of the special selectors: qa, cond
  VALUE             The new value for the specified keyword

  NOTE: If selecting by date/filenums, the selected files will be in the form

          NYYYYMMDDSxxxx.fits

  where 'xxxx' is a number generated from the ranges in <filenums>

Values for Special Selectors:

  qa    The value can be one of 'undefined', 'pass', 'usable', 'fail', 'check'
  cond  The value is a comma-separated list of site conditions to be changed,
        like 'iqany', 'wv80', 'cc50', etc.

Optional Arguments:

  -h, --help     This help message
  -d, --dry-run  Show the potential changes, but don't perform them
  -y             "Yes, I'm sure!" Pass this flag when setting a keyword that
                 doesn't exist in the original file. If not passed, you will
                 get an error message (this is a protection against typos)

  --ext=EXT      EXT is a number greater than 0 or an extension name. If not
                 specified, it is assumed that the change affects the
                 Primary HDU
"""

import sys
from time import strptime
from datetime import datetime, timedelta

ISODATEFORMAT='%Y%m%d'

def usage(with_error = True):
    print __doc__
    sys.exit(1 if with_error else 0)

def yesterday():
    return (datetime.today() - timedelta(days = 1)).strftime(ISODATEFORMAT)

# This script has a complex argument parsing that is easier to do manually
# instead of using argparse
def parse_args(raw_args):
    class SetLimitError(Exception):
        pass

    class Args(object):
        def __new__(typ, *args, **kw):
            obj = object.__new__(typ, *args, **kw)
            obj.__dict__['_{}__set_once'.format(typ.__name__)] = {}

            return obj

        def __init__(self, **defaults):
            for (attr, value) in defaults.items():
                setattr(self, attr, value)
                self.__set_once[attr] = False

        def __setattr__(self, attr, value):
            try:
                if self.__set_once[attr]:
                    raise SetLimitError(attr)
                self.__set_once[attr] = True
            except KeyError:
                pass

            super(Args, self).__setattr__(attr, value)

        def __str__(self):
            return "Args({})".format(', '.join('{}={!r}'.format(k, getattr(self, k)) for k in sorted(self.__set_once)))

    args = Args(yes=False, dry_run=False, ext=None,
                date=None, filenums=None, obsid=None,
                pairs=[])

    # Just a shallow copy of the arguments
    rargs = raw_args[:]

    try:
        # First look for the options
        while rargs and rargs[0].startswith('-'):
            rarg = rargs.pop(0)
            if rarg in ('-h', '--help'):
                usage(with_error = False)
            elif rarg in ('-d', '--dry-run'):
                args.dry_run = True
            elif rarg == '-y':
                args.yes = True
            elif rarg.startswith('--ext='):
                _, _, args.ext = rarg.partition('=')
            else:
                usage()


        narg = rargs.pop(0)

        if narg == 'obsid':
            args.obsid = rargs.pop(0)
        else:
            try:
                strptime(narg, ISODATEFORMAT)
                args.date = narg
                args.filenums = rargs.pop(0)
            except ValueError:
                args.date = yesterday()
                args.filenums = narg

        # Process the remaining arguments
        for pair in rargs:
            kw, _, value = pair.partition(':')
            if _ != ':' or not value:
                usage()
            args.pairs.append((kw, value))

    except (SetLimitError, IndexError):
        usage()

    if (not args.obsid and not args.filenums) or not args.pairs:
        usage()

    return args

if __name__ == '__main__':

    args = parse_args(sys.argv[1:])
    print args
