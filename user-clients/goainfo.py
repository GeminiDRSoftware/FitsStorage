#! /usr/bin/env python

#************************************************************************
#****              G E M I N I  O B S E R V A T O R Y                ****
#************************************************************************
#
#   Script name:        goainfo
#
#   Purpose:
#      Query the Gemini Archive to check for presence of a file
#
#   Date               : 2015-10-14
#
#   Author             : Ricardo Cardenes
#
#   Modification History:
#    2015-10-14, rcardene : First release
#

from contextlib import closing
from datetime import datetime
import json
import logging
import sys
import urllib

STANDARD_SERVER='archive.gemini.edu'

def grab_info_from_url(url):
    """
    Performs a query against a web service expecting some JSON object to
    be returned.

    Transform the JSON string into a Python object, and return it to the
    caller
    """

    # Note to Python newbies: urllib.urlopen returns a "file-like" object, meaning that
    # you can read from it like if it were a file, and you're expected to close it.
    # We use here a 'with' block to do just that (stuff created in the 'with' sentence
    # is cleaned up at the end of the block, no matter how we exit from it), but the
    # object returned by urllib is not smart enough to be used straight here.
    #
    # But we can wrap the object with contextlib.closing to help with that
    try:
        with closing(urllib.urlopen(url)) as response:
            status = response.getcode()
            if status == 200:
                return json.loads(response.read())
            else:
                logging.error("Got some non-specific error when querying the server. Report this!")
    # We get ValueError if the query returns a non-valid JSON object.
    # We get the other two errors if something went wrong with querying the web server
    except ValueError:
        logging.error("Could not get retrieve valid information from the server")
    except IOError:
        logging.error("Could not contact the web server!")

def strip_datetime_extras(datetimestring):
    """
    Takes a string containing date & time string and removes decimal seconds from it
    (We don't need such precision), and also timezone data

    It also adds a 'T' to separate date and time, to make it more compliant with
    ISO 8601 -and in the process we remove that pesky space, which would make it more
    difficult to extract fields from the listings
    """

    # The UT datetime strings may come with +00:00 at the end (time zone offset).
    # strptime doesn't support time zones, so we're going to try and partition
    # the string upon finding "+". If there's no "+", the second and third
    # variables will be set to ''. In any case, we don't care about their values,
    # so we just use _ as a placeholder for them.
    dt, _, _ = datetimestring.partition('+')

    return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%dT%H:%M:%S")

# We're printing most of the information straight from what we get from the JSON
# object, but we may want to format some stuff in other ways...
#
# What we do is to provide a filtering function, that takes the raw value and
# returns the formatted output. This will be indexed by field name. If the field
# does not appear in the dictionary, then we don't do anything and the raw value
# will be used as normal.
extra_formatting = {
    'lastmod': strip_datetime_extras,
    'ut_datetime': strip_datetime_extras,
    }

def print_info(filelist, metadata_to_print):
    formatlist = []
    for field in metadata_to_print:
        formatlist.append('{' + field + '}')

    formatstring = ' '.join(formatlist)
    for fileinfo in filelist:
        for field in metadata_to_print:
            if field in extra_formatting:
                fileinfo[field] = extra_formatting[field](fileinfo[field])
        # Note: The **argument syntax means "argument is a dictionary; expand it
        #       as pass-by-name arguments.
        #
        # E.g., if we have a dictionary d={'a': 1, 'b': 2}, function(**d) is
        #       equivalente to function(a=1, b=2)
        print(formatstring.format(**fileinfo))

def get_file_info(arguments, server=STANDARD_SERVER):
    """
    Queries the Archive looking for info about specific files
    """
    criteria_string = '/'.join(arguments)
    query = 'http://{server}/jsonsummary/{criteria}'.format(server=server, criteria=criteria_string)

    return grab_info_from_url(query)

epilog = """
The script will return 0 as status code if it can find at least one file matching
the search criteria; 1 otherwise. If you only need this information, you can use
the --quiet argument, instead of discarding output.

The basic information table is "file-name last-modification-date". If --filedetails is
selected, extra fields are added (see help above).

If --summary is selected, the list of fields printed will be:

  file-name datalabel observation-type ut-datetime qa-state
"""

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Query the Gemini Archive to check for presence of a file',
                                     epilog=epilog)
    parser.add_argument('--filedetails', dest='details', action='store_true',
                        help="Additionally, print MD5 sum and size of the uncompressed file")
    parser.add_argument('--summary', dest='summary', action='store_true',
                        help="Print a basic summary table")
    parser.add_argument('--quiet', dest='quiet', action='store_true',
                        help="Print no info")
    parser.add_argument('criteria', metavar='search-criterion', nargs='+',
                        help="File name prefix, instrument, date, program ID, etc...")

    options = parser.parse_args()

    info = get_file_info(options.criteria)
    # This will evaluate to False in the case that info is empty, or we got hit
    # some error when querying the server
    if not info:
        sys.exit(1)

    if not options.quiet:
        if options.summary:
            metadata = ('name', 'data_label', 'observation_type', 'ut_datetime', 'qa_state')
        else:
            metadata = ('name', 'lastmod')
            if options.details:
                metadata = metadata + ('data_md5', 'data_size')
        print_info(info, metadata)

        if 'results_truncated' in info[-1]:
            print("\n\nWARNING!! Your search was too broad and the results have been truncated!")

    sys.exit(0)
