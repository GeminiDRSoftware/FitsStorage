#! /usr/bin/env python

from __future__ import print_function

"""
FITS Header Fixing Tool v0.7

Usage:

 fixHead [-hlNSy] [--ext=EXT] [<date>] <filenums> KEYW:VAL [KEYW:VAL ...]
 fixHead [-hly]   [--ext=EXT] obsid <observation-id> KEYW:VAL [KEYW:VAL ...]

Arguments:

  <date>            Optional, in format YYYYMMDD. If not specified, the current
                    date is assumed.
  <filenums>        A comma-separated list of number ranges. Eg: 10-20,31,35-40
  <observation-id>  A valid Gemini observation ID
  KEYW              A keyword or one of the special selectors: qa, cond
  VAL               The new value for the specified keyword

 NOTE: If selecting by date/filenums, the selected files will be in the form

          NYYYYMMDDSxxxx.fits

 where 'xxxx' is a number generated from the ranges in <filenums>

Values for Special Selectors:

  qa    The value can be one of 'undefined', 'pass', 'usable', 'fail', 'check'
  cond  The value is a comma-separated list of site conditions to be changed,
        like 'iqany', 'wv80', 'cc50', etc.

Optional Arguments:

  -h, --help     This help message
  -l, --list     Show a list of files that would be changed, but do not perform
                 changes. When using -l, the KEYW:VAL pairs are not mandatory
  -y             "Yes, I'm sure!" Pass this flag when setting a keyword that
                 doesn't exist in the original file. If not passed, you will
                 get an error message (this is a protection against typos)
  -N             Force selection Gemini North files
  -S             Force selection Gemini South files
  --ext=EXT      EXT is a number greater than 0 or an extension name. If not
                 specified, it is assumed that the change affects the
                 Primary HDU

Usage examples:

 fixHead 20160116 15,20,25 RAWIQ:70 qa:usable
 fixHead 20160115 10-20,31,35-40 RAWIQ:any RAWWV:80 RAWCC:50
 fixHead 20160115 10-20,31,35-40 cond:iqany,wv80,cc50
 fixHead 1-200 SSA:"John Smith"
"""



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
#    2016-02-11, rcardene : Change the default server. Remove the cookie
#                           which should not be in the code repository
#    2016-10-10, rcardene : Introduced some rudimentary checks for known fixed
#                           values (RAWxx, etc). Improved help messages
#    2016-11-05, rcardene : Allow fixHead to pick the API cookie from the environment
#    2016-12-14, rcardene : Clarified error message for non-allowed input values
#    2016-12-16, mpohlen  : Updated history and version number
#    2016-12-20, rcardene : Updated history with meaningful dates and bumped
#                           version to 0.5
#    2017-01-11, rcardene : Fixed a bug with date acquisition. Released as 0.6
#    2017-04-06, rcardene : Re-fixed the bug with date acquisition. Released as 0.7

import sys
from time import strptime
from datetime import datetime, timedelta
import json
import requests
from requests.exceptions import ConnectionError
from functools import partial
import os

SERVERNAME='fits'
NORTHPREF = 'N'
SOUTHPREF = 'S'
DEFAULTPREF = NORTHPREF
ISODATEFORMAT='%Y%m%d'

# Pick the Gemini API authorization cookie from the environment
# If it's not there, we get an empty string. This won't get you
# access to the archive, but at least the script won't crash.
GEM_AUTH = os.environ.get('GEMINI_API_AUTH', '')

cookies = {
    'gemini_api_authorization': GEM_AUTH
}

class ServerAccess(object):
    """
    Class that abstracts queries to the archive server
    """

    def __init__(self, host, port = '80'):
        self.server = '{}:{}'.format(host, port)

    def uri(self, *extra):
        return '/'.join(['http://{}'.format(self.server)] + list(extra))

    def summary(self, *selection):
        return requests.get(self.uri('jsonfilenames/present', *selection)).json()

    def batch_change(self, file_list, actions, reject_new):
        arguments = {'request': [{'filename': fn, 'values': actions, 'reject_new':reject_new} for fn in file_list],
                     'batch': False}

        return requests.post(self.uri('update_headers'),
                             json=arguments,
                             cookies=cookies)

def get_file_list_by_obsid(server, args):
    """
    Contacts the server asking for a list of files matching the provided
    observation id, and returns a tuple that information.
    """
    return tuple(info['name'] for info in server.summary('obsid='+args.obsid))

def get_file_list_by_date_and_number(args):
    """
    Returns a tuple with a list of files generated based on the provided numbers,
    date, and prefix.
    """
    pref = args.prefix or DEFAULTPREF
    return tuple('{}{}S{:04d}.fits'.format(pref, args.date, n) for n in args.filenums)

def get_file_list(server, args):
    if args.obsid:
        return get_file_list_by_obsid(server, args)
    else:
        return get_file_list_by_date_and_number(args)

doc_simplified ="""
Usage examples for SOS purposes:

 {GREEN}fixHead 20160116 15,20,25 RAWIQ:70 qa:usable{RESET}
 {GREEN}fixHead 20160115 10-20,31,35-40 RAWIQ:any RAWWV:80 RAWCC:50{RESET}

That last one can also be written:

 {GREEN}fixHead 20160115 10-20,31,35-40 cond:iqany,wv80,cc50{RESET}

Specials:

  'cond' accepts up to 4 values separated by commas, like "iq70,bgany"
  'qa' accepts the values that you expect for QA, like "pass", "check",
       etc. The server will translate this to the proper values in the
       headers.
  For RAWBG/RAWCC/RAWIQ/RAWWV you can specify anything that contains a
  number (eg. RAWIQ:70, RAWWV:80-per). As long as it is a valid number,
  it will be completed to something proper, like '80-percentile'

You can skip the date (the current one will be used):

 {GREEN}fixHead 1-200 SSA:"John Smith"{RESET}

Type "fixHead -h" for a more thorough help message.
"""

def colorize(text):
    colors = {
         'RED': '1',
         'GREEN': '2',
         'YELLOW': '3',
         'MAGENTA': '5',
         'CYAN': '6',
         'RESET': None,
    }
    for (tag, code) in list(colors.items()):
        if tag is 'RESET':
            color = '\x1b[0m'
        else:
            color = '\x1b[3{0};1m'.format(code)
        text = text.replace('{' + tag + '}', color)
    return text

def usage(with_error = True, full = True):
    if full:
        print(__doc__)
    else:
        print(colorize(doc_simplified))
    sys.exit(1 if with_error else 0)

def expand_numbers(nums):
    groups = nums.split(',')
    rng = []
    for group in groups:
        if group.isdigit():
            n = int(group)
            rng.extend(list(range(n, n+1)))
        else:
            # If there are not exactly two elements in the range, this will raise a
            # ValueError
            n1, n2 = group.split('-')
            rng.extend(list(range(int(n1), int(n2)+1)))

    return rng

def today_hawaii():
    return datetime.utcnow().strftime(ISODATEFORMAT)

def yesterday():
    return (datetime.today() - timedelta(days = 1)).strftime(ISODATEFORMAT)

# This script has a complex argument parsing that is easier to do manually
# instead of using argparse
def parse_args(raw_args):
    class SetLimitError(Exception):
        pass

    class Args(object):
        def __new__(typ, *args, **kw):
            obj = object.__new__(typ)
            obj.__dict__['_{}__set_once'.format(typ.__name__)] = {}

            return obj

        def __init__(self, **defaults):
            for (attr, value) in list(defaults.items()):
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

    if not raw_args:
        usage(full=False)

    args = Args(yes=False, show_list=False, ext=None,
                date=None, filenums=None, obsid=None,
                prefix=None, pairs=[],
                today=today_hawaii)

    # Just a shallow copy of the arguments
    rargs = raw_args[:]

    try:
        # First look for the options
        while rargs and rargs[0].startswith('-'):
            rarg = rargs.pop(0)
            if rarg in ('-h', '--help'):
                usage(with_error = False)
            elif rarg in ('-l', '--list'):
                args.show_list = True
            elif rarg == '-y':
                args.yes = True
            elif rarg == '-N':
                args.prefix = NORTHPREF
            elif rarg == '-S':
                args.prefix = SOUTHPREF
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
                nums = rargs.pop(0)
            except ValueError:
                args.date = args.today()
                nums = narg

            args.filenums = expand_numbers(nums)

        # Process the remaining arguments
        for pair in rargs:
            kw, _, value = pair.partition(':')
            if _ != ':' or not value:
                usage()
            args.pairs.append((kw, value))

    except (SetLimitError, IndexError, ValueError):
        usage()

    if (not args.obsid and not args.filenums) or (not args.pairs and not args.show_list):
        usage()

    return args

class NotInRangeError(ValueError):
    def __init__(self, msg, valid):
        super(NotInRangeError, self).__init__(msg)
        self.valid = valid

def validate_raw(inp, accepted, keyw):
    if inp.lower() == 'any':
        return 'Any'
    elif inp.upper().startswith('UNK'):
        return 'UNKNOWN'
    else:
        for number in accepted:
            if str(number) in inp:
                return '{}-percentile'.format(number)

    raise NotInRangeError("{key} does not accept '{value}' as an input".format(key=keyw, value=inp),
            valid=tuple('{}-percentile'.format(x) for x in accepted) + ('Any', 'UNKNOWN'))

def validate_keyword(inp, accepted, keyw, trans=str):
    transformed_value = trans(inp)
    if transformed_value not in accepted:
        raise NotInRangeError("{key} does not accept '{value}' as an input".format(key=keyw, value=inp),
                valid=accepted)

    return transformed_value

valid_sets = {
        'qa': ('undefined', 'pass', 'usable', 'fail', 'check'),
        'RAWGEMQA': ('UNKNOWN', 'USABLE', 'BAD', 'CHECK'),
        'RAWPIREQ': ('UNKNOWN', 'YES', 'NO', 'CHECK')
        }

def map_actions(pairs):
    actions = {}
    gen = []
    for keyword, value in pairs:
        keyword = keyword.lower()
        if keyword == 'qa':
            actions['qa_state'] = validate_keyword(value, valid_sets['qa'], 'qa', trans=str.lower)
        elif keyword == 'cond':
            actions['raw_site'] = value
        elif keyword == 'release':
            actions['release'] = value
        else:
            keyword = keyword.upper()
            if keyword == 'RAWIQ':
                value = validate_raw(value, (20, 70, 85), keyword)
            elif keyword == 'RAWCC':
                value = validate_raw(value, (50, 70, 80), keyword)
            elif keyword in ('RAWBG', 'RAWWV'):
                value = validate_raw(value, (20, 50, 80), keyword)
            elif keyword in ('RAWGEMQA', 'RAWPIREQ'):
                value = validate_keyword(value, valid_sets[keyword], keyword, trans=str.upper)

            gen.append((keyword.upper(), value))
    if gen:
        actions['generic'] = gen

    return actions

def perform_changes(sa, file_list, args):
    try:
        ret = sa.batch_change(file_list, map_actions(args.pairs), not args.yes)
    except ConnectionError:
        print("Cannot connect to the archive server", file=sys.stderr)
        return 1
    except NotInRangeError as e:
        print(colorize('{RED}{}{RESET}').format(str(e)), file=sys.stderr)
        print('Valid inputs are:', file=sys.stderr)
        for vi in e.valid:
            print(colorize('   {GREEN}{}{RESET}').format(vi), file=sys.stderr)
        return 0
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 0

    if ret.status_code == 403:
        print("The access to the archive server has been forbidden for this script, or you do not have the auth cookie set in your environment.", file=sys.stderr)
        return 2

    for response in ret.json():
        if not response['result']:
            print('{}: {}'.format(response.get('id', 'UNKNOWN'), response['error']))
        else:
            print('{} modified successfully'.format(response['id']))

    return 0

if __name__ == '__main__':
    from pprint import pprint
    args = parse_args(sys.argv[1:])
    sa = ServerAccess(SERVERNAME)
    try:
        file_list = get_file_list(sa, args)
    except ConnectionError:
        print("Cannot connect to the archive server", file=sys.stderr)
        sys.exit(1)
    else:
        if args.show_list:
            print("The following files would be affected:")
            for fname in file_list:
               print("  " + fname)
        else:
            sys.exit(perform_changes(sa, file_list, args))
