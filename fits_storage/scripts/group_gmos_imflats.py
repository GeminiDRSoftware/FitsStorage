#! /usr/bin/env python3

from argparse import ArgumentParser
import datetime

import fits_storage.gemini_metadata_utils as gmu

from sqlalchemy import select, func

from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import sessionfactory

from fits_storage.server.orm.monitoring import Monitoring

def main():
    # Option Parsing
    parser = ArgumentParser()
    parser.add_argument("--start", action="store",
                        help="Start date")
    parser.add_argument("--end", action="store",
                        help="End date")
    parser.add_argument("--tag", action="store",
                        help="Processing tag to search on")
    parser.add_argument("--instrument", action="store",
                        default="GMOS-N", help="Instrument to search")
    parser.add_argument("--filter", action="store",
                        help="filter Name to search on"),
    parser.add_argument("--binning", action="store",
                        default="2x2",
                        help="Detector binning to search on [2x2]")
    parser.add_argument("--keyword", action="store",
                        default='FLATMED', help="Keyword to use [FLATMED]")
    parser.add_argument("--debug", action="store_true",
                        help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true",
                        help="Run as a background demon, do not generate "
                             "stdout")
    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    nights = get_gmos_imflats(options.start, options.end, options.tag,
                              options.keyword, options.instrument,
                              options.filter, options.binning)

    groups = group_imflats(nights)
    return 0

def get_gmos_imflats(start=None, end=None, tag=None, keyword=None,
                       instrument=None, filter=None, binning=None):
    # Announce startup
    logger.info("***  group_gmos_imflats.py - starting up at %s"
                % datetime.datetime.now())

    session = sessionfactory()

    # There is one FLATMED value for each extension in each file, find the
    # average of these for each file that matches.
    # We only select header_ids from canonical diskfiles here. This relies on
    # a new monitoring entry being generated if a new version of the diskfile
    # is uploaded, which relies on that being configures in reduce_on_ingest
    stmt = (select(Monitoring.header_id, func.avg(Monitoring.value_float)).
            select_from(Monitoring, Header, DiskFile).
            where(Monitoring.header_id == Header.id).
            where(Header.diskfile_id == DiskFile.id).
            where(DiskFile.canonical).
            where(Header.instrument == instrument).
            where(Monitoring.keyword == keyword)
            .group_by(Monitoring.header_id))

    if tag:
        stmt = stmt.where(Monitoring.processing_tag == tag)

    if start:
        stmt = stmt.where(Monitoring.ut_datetime > start)

    if end:
        stmt = stmt.where(Monitoring.ut_datetime < end)

    if filter:
        stmt = stmt.where(Header.filter_name == filter)

    if binning:
        stmt = stmt.where(Header.detector_binning == binning)

    results = session.execute(stmt).all()

    # OK, we're going to build a list of nights, and total counts.
    # Do this on the python side for now.
    nights = {}
    for (hid, counts) in results:
        header = session.get(Header, hid)
        # Get the night
        if header.telescope == 'Gemini-North':
            night = header.ut_datetime.date()
        elif header.telescope == 'Gemini-South':
            night = (header.ut_datetime + gmu.CHILE_OFFSET).date()
        else:
            continue
        night = _tostr(night) if night else None
        if night not in nights:
            nights[night] = 0
        nights[night] += counts

    return nights

def group_imflats(nights, interactive=True):
    # Groups is a list of dicts
    groups = []
    if interactive:
        cmd = None
        while cmd != 'Q':
            print(f"\nNight: {len(nights)}, Groups: {len(groups)}")
            try:
                cmd = input("[n]ights (ungrouped) [g]roups. [A]uto group [threshold]"
                            "[a]dd night [group] [s]ave filename "
                            "[Q]uit (done): ").strip()
            except EOFError:
                # Ctrl-D - treat as Quit
                print()
                cmd = 'Q'
            things = cmd.split()
            if cmd == 'n':
                for night in nights:
                    print(f"{night}: {nights[night]:,.1f}")
            elif cmd == 'g':
                for group in groups:
                    print(f"{group}")
            elif things and things[0] == 'A':
                if len(things) == 1:
                    autogroup(nights, groups)
                elif len(things) == 2:
                    try:
                        thresh = float(things[1])
                    except ValueError:
                        print("Usage: A [threshold]")
                        continue
                    autogroup(nights, groups, thresh)
                else:
                    print("Usage: A [threshold]")
                    continue
            elif things and things[0] == 'a':
                if len(things) == 2:
                    addntog(nights, groups, things[1])
                elif len(things) == 3:
                    addntog(nights, groups, things[1], things[2])
                else:
                    print("bad command - usage: a night [group]")
            elif things and things[0] == 's':
                if len(things) != 2:
                    print("Usage: s <filename>")
                    continue
                savegroups(groups, things[1])
            elif cmd == 'Q':
                pass
            else:
                print(f"Unknown command {cmd}")
        print("Quit")
    return groups

def addntog(nights, groups, n, g=None):
    # Nights with an unrecognised telescope are stored under None, allow
    # them to be referred to as 'None' from the command line.
    if n == 'None' and None in nights:
        n = None
    if n not in nights:
        print(f"Night {n} not found")
        return
    if g is None:
        # Add night as a new group
        value = nights.pop(n)
        group = {'id': len(groups), 'nights': [n], 'total': value}
        groups.append(group)
    else:
        try:
            gid = int(g)
        except ValueError:
            print(f"Bad group id {g} - must be an integer")
            return
        for group in groups:
            if group['id'] == gid:
                group['nights'].append(n)
                group['total'] += nights.pop(n)
                return
        print(f"Group {gid} not found")


def autogroup(nights, groups, thresh=1E6):
    for night in list(nights):
        if nights[night] > thresh:
            logger.info(f"Auto - {nights[night]:,.1f} > {thresh}: making single night group")
            addntog(nights, groups, night)
        else:
            logger.info(f"Auto - {nights[night]:,.1f} < {thresh}, passing for now")

def savegroups(groups, filename):
    with open(filename, 'a') as fp:
        for group in groups:
            # This makes the possibly naive assumption that groups are not
            # discontinuous
            dates = [_todate(d) for d in group['nights']]
            mindate = _tostr(min(dates))
            maxdate = _tostr(max(dates))
            fp.write(f"[{mindate}-{maxdate}]\n")

def _tostr(d):
    return d.strftime('%Y%m%d')

def _todate(s):
    # TODO: python 3.14 adds a date.strptime() method
    return datetime.datetime.strptime(s, '%Y%m%d').date()

if __name__ == '__main__':
    exit(main())
