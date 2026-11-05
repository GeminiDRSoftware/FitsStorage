#! /usr/bin/env python3


import datetime
from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import sessionfactory
from fits_storage.db.selection.get_selection import from_url_things
from fits_storage.db.list_headers import list_headers


def main():
    # Option Parsing
    parser = ArgumentParser()
    parser.add_argument("--instrument", action="store",
                        help="Instrument name to search")
    parser.add_argument("--startdate", action="store",
                        help="Start date to find frames")
    parser.add_argument("--enddate", action="store",
                        help="End date to find frames")
    parser.add_argument("--filter", action="store",
                        help="filter_name")
    parser.add_argument("--binning", action="store",
                        help="binning NxN")
    parser.add_argument("--outfile", action="store",
                        help="File to write output to")
    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run as a background demon, do not generate "
                             "stdout")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("***   generate_fringe_frame_reduce_list.py - starting up at %s"
                % datetime.datetime.now())

    groups = find_fringe_frames(instrument=options.instrument,
                       start=options.startdate, end=options.enddate,
                       filter=options.filter, binning=options.binning)

    write_outputs(groups, options.outfile)

def find_fringe_frames(instrument=None, start=None, end=None,
                       filter=None, binning=None):
    session = sessionfactory()

    # We use the selection url things for convenience here
    things = [instrument, 'canonical', 'notengineering', 'NotFail', 'imaging', 'fullframe', 'low', 'slow', 'Raw', 'RAW', 'partnerCal', 'OBJECT', 'object=Blank Field']
    if start and end:
        things.append(f"{start}-{end}")
    if filter:
        things.append(filter)
    if binning:
        things.append(binning)
    selection = from_url_things(things)

    headers = list_headers(selection, ['ut_datetime'], session=session, unlimit=True)

    # We're going to group these by observation id. All we want is the filenames
    groups = {}
    for header in headers:
        obs_id = header.observation_id
        if obs_id not in groups.keys():
            groups[obs_id] = []
        filename = header.diskfile.filename
        groups[obs_id].append(filename)
        logger.debug(f"{obs_id}: {filename}")

    return groups

def write_outputs(groups, filename):
    if filename:
        with open(filename, 'w+') as fp:
            for group in groups:
                fp.write(f'# {group}: {len(groups[group])} files\n')
                fp.write(' '.join(groups[group]))
                fp.write('\n\n')

    for group in groups:
        logger.info(f'# {group}: {len(groups[group])} files')
        
if __name__ == '__main__':
    main()