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
            where(Header.mode == 'imaging').
            where(Header.processing == 'Raw').
            where(Header.reduction == 'RAW').
            where(Header.observation_type == 'OBJECT').
            where(Header.detector_roi_setting == 'Full Frame').
            where(Header.detector_gain_setting == 'low').
            where(Header.detector_readspeed_setting == 'slow').
            where(Header.object == 'Twilight').
            where(Header.qa_state.in_(('Pass', 'Undefined'))).
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
                cmd = input("[n]ights (ungrouped) [g]roups [A]uto group "
                            "[threshold [maxgap]] [a]dd night [group] "
                            "[m]ulti add night1 night2 ... (makes new group) "
                            "[f]ind groups near night [s]ave filename "
                            "[Q]uit (done): ").strip()
            except EOFError:
                # Ctrl-D - treat as Quit
                print()
                cmd = 'Q'
            things = cmd.split()
            if cmd == 'n':
                # Night keys are YYYYMMDD strings, so these sort by date
                for night in sorted(nights):
                    print(f"{night}: {nights[night]:,.1f}")
            elif cmd == 'g':
                # Display in date order of first night. We don't renumber
                # the groups as the ids are used in the 'a' command.
                for group in _sortedgroups(groups):
                    print(_groupstr(group))
            elif things and things[0] == 'A':
                usage = "Usage: A [threshold [maxgap]]"
                if len(things) > 3:
                    print(usage)
                    continue
                kwargs = {}
                try:
                    if len(things) > 1:
                        kwargs['thresh'] = float(things[1])
                    if len(things) > 2:
                        kwargs['maxgap'] = int(things[2])
                except ValueError:
                    print(usage)
                    continue
                autogroup(nights, groups, **kwargs)
            elif things and things[0] == 'a':
                if len(things) == 2:
                    addntog(nights, groups, things[1])
                elif len(things) == 3:
                    addntog(nights, groups, things[1], things[2])
                else:
                    print("bad command - usage: a night [group]")
            elif things and things[0] == 'm':
                if len(things) > 2:
                    addntog(nights, groups, things[1:])
                else:
                    print("bad command - usage: m night1 night2 ...")
            elif things and things[0] == 'f':
                if len(things) != 2:
                    print("Usage: f night")
                    continue
                findgroups(nights, groups, things[1])
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
    """
    Add night n to group g. If g is None, make a new group containing night n.
    If n is a list of nights, make a new group containing all of them (g is
    ignored). That's all or nothing - if any of the nights are not available
    we don't make the group.
    """
    if isinstance(n, list):
        # Make new group from list of nights
        missing = [x for x in n if x not in nights]
        if missing:
            print(f"Night(s) not found: {', '.join(missing)} - "
                  f"no group created")
            return
        if len(set(n)) != len(n):
            print("Night(s) listed more than once - no group created")
            return
        total = sum(nights.pop(x) for x in n)
        group = {'id': len(groups), 'nights': sorted(n), 'total': total}
        groups.append(group)
        return

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
                group['nights'].sort()
                group['total'] += nights.pop(n)
                return
        print(f"Group {gid} not found")


def autogroup(nights, groups, thresh=1E6, maxgap=3):
    """
    Pass 1: any night with total counts over thresh becomes a single night
    group.
    Pass 2: go through the remaining nights in date order, and make groups
    out of runs of neighbouring nights whose combined counts exceed thresh.
    Neighbouring nights can be at most maxgap nights apart (ie with at most
    maxgap empty nights between them - maxgap=0 means strictly consecutive),
    and a run never spans a night that is already in a group.
    Pass 3: runs of leftover nights that never reached the threshold are
    added to the nearest neighbouring group (before or after), provided it's
    within maxgap. Otherwise they're left ungrouped.
    """
    for night in list(nights):
        if nights[night] > thresh:
            logger.info(f"Auto - {nights[night]:,.1f} > {thresh}: making single night group")
            addntog(nights, groups, night)
        else:
            logger.info(f"Auto - {nights[night]:,.1f} < {thresh}, passing for now")

    # Now run through the night list again, and make groups out of consecutive
    # nights where we can meet the threshold
    grouped = sorted(_todate(n) for g in groups for n in g['nights'])
    run = []
    total = 0
    prev = None
    leftovers = []
    for night in sorted(nights):
        date = _todate(night)
        if prev is not None:
            gap = (date - prev).days - 1
            spans_group = any(prev < g < date for g in grouped)
            if gap > maxgap or spans_group:
                if run:
                    leftovers.append(run)
                run, total = [], 0
        run.append(night)
        total += nights[night]
        prev = date
        if total > thresh:
            logger.info(f"Auto - grouping {' '.join(run)}: "
                        f"{total:,.1f} > {thresh:,.1f}")
            addntog(nights, groups, run)
            grouped.extend(_todate(n) for n in run)
            grouped.sort()
            run, total, prev = [], 0, None
    if run:
        leftovers.append(run)

    # Pass 3: add leftover runs to the nearest neighbouring group, if within
    # maxgap. Leftover runs never span a grouped night, so the nearest grouped
    # night before / after the run is the neighbouring group.
    group_of = {_todate(n): g['id'] for g in groups for n in g['nights']}
    for run in leftovers:
        first, last = _todate(run[0]), _todate(run[-1])
        before = [d for d in group_of if d < first]
        after = [d for d in group_of if d > last]
        candidates = []  # (gap, group id)
        if before:
            candidates.append(((first - max(before)).days - 1,
                               group_of[max(before)]))
        if after:
            candidates.append(((min(after) - last).days - 1,
                               group_of[min(after)]))
        # Nearest group wins, ties go to the earlier group
        candidates = [c for c in candidates if c[0] <= maxgap]
        if candidates:
            gap, gid = min(candidates, key=lambda c: c[0])
            logger.info(f"Auto - adding leftover {' '.join(run)} to group "
                        f"{gid} (gap {gap})")
            for night in run:
                addntog(nights, groups, night, gid)
        else:
            logger.info(f"Auto - leftover {' '.join(run)} not within "
                        f"{maxgap} of a group, leaving ungrouped")


def findgroups(nights, groups, night, n=1):
    """
    Show the n nearest groups on either side of night. night can be any date
    in YYYYMMDD format - it doesn't have to be in the nights list.
    """
    try:
        date = _todate(night)
    except ValueError:
        print(f"Bad night {night} - must be YYYYMMDD")
        return

    if night in nights:
        print(f"Night {night} is ungrouped: {nights[night]:,.1f}")

    before, during, after = [], [], []
    for group in groups:
        first = _todate(group['nights'][0])
        last = _todate(group['nights'][-1])
        if last < date:
            before.append(((date - last).days - 1, group))
        elif first > date:
            after.append(((first - date).days - 1, group))
        else:
            # night is within this group's date range
            during.append(group)

    for group in during:
        where = "in" if night in group['nights'] else "within the dates of"
        print(f"Night {night} is {where} group: {_groupstr(group)}")

    print("Before:")
    # Nearest first
    for gap, group in sorted(before, key=lambda x: x[0])[:n]:
        print(f"  gap {gap}: {_groupstr(group)}")
    if not before:
        print("  (none)")
    print("After:")
    for gap, group in sorted(after, key=lambda x: x[0])[:n]:
        print(f"  gap {gap}: {_groupstr(group)}")
    if not after:
        print("  (none)")

def _groupstr(group):
    return (f"{group['id']}: {' '.join(group['nights'])} "
            f"total={group['total']:,.1f}")

def savegroups(groups, filename):
    with open(filename, 'a') as fp:
        for group in _sortedgroups(groups):
            # This makes the possibly naive assumption that groups are not
            # discontinuous
            dates = [_todate(d) for d in group['nights']]
            mindate = _tostr(min(dates))
            maxdate = _tostr(max(dates))
            fp.write(f"[{mindate}-{maxdate}]\n")

def _sortedgroups(groups):
    # Groups in date order of their first night. Each group's nights list
    # is kept sorted, so the first night is the earliest.
    return sorted(groups, key=lambda g: g['nights'][0])

def _tostr(d):
    return d.strftime('%Y%m%d')

def _todate(s):
    # TODO: python 3.14 adds a date.strptime() method
    return datetime.datetime.strptime(s, '%Y%m%d').date()

if __name__ == '__main__':
    exit(main())
