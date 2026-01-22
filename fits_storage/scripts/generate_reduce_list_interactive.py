#! /usr/bin/env python3

import datetime

from fits_storage.config import get_config
fsc = get_config()

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix

from fits_storage.server.reduce_list import ReduceListConfig, findfiles

oneday = datetime.timedelta(days=1)


class Block(object):
    start = None
    end = None
    files = []

    def __init__(self, start, end, files):
        self.start = start
        self.end = end
        self.files = files

    def __repr__(self):
        return f"{self.start.isoformat()} - {self.end.isoformat()}: {len(self.files)}"

    def repopulate(self, bydate):
        # Re-populate the file list after a date change, using bydate dict
        self.files = []
        d = self.start
        while d <= self.end:
            if d in bydate.keys():
                self.files += bydate[d]
            else:
                print(f"Don't have files for {d.isoformat()}, skipping")
            d += oneday

class BlockList(object):
    def __init__(self, selection, start, end, num):
        self.selection = selection
        self.start = start
        self.end = end
        self.blocks = []
        self.bydate = {}
        self.num = num

        self.populate_bydate()
        self.build()

    def print(self):
        for block in self.blocks:
            print(f"block {self.blocks.index(block)}: {block}")


    def updateblock(self, cmd):
        se = cmd[0]
        pm = cmd[1]
        b = int(cmd[2:])
        block = self.blocks[b]
        adj = -1 * oneday if pm == '-' else oneday
        if se.lower() == 's':
            block.start += adj
        else:
            block.end += adj
        block.repopulate(self.bydate)
        if se == 'E':
            # Update subsequent blocks both start and end
            b += 1
            while b < len(self.blocks):
                block = self.blocks[b]
                block.start += adj
                block.end += adj
                block.repopulate(self.bydate)
                b += 1

    def populate_bydate(self, d=None):
        if d is None:
            d = start
            logger.info("Fetching filename lists")
            while d <= end:
                self.bydate[d] = findfiles(self.selection, d, None, logger=logger)
                d += oneday
        else:
            logger.info(f"Fetching filename list for {d.isoformat()}")
            self.bydate[d] = findfiles(self.selection, d, None, logger=logger)

    def build(self):
        # Build an initial block list
        self.blocks = []
        d = start
        newblock = True
        while d <= end:
            if newblock:
                bstart = d
                bfiles = []
                newblock = False
            bfiles += self.bydate[d]
            if len(bfiles) >= self.num or d == end:
                # This block is complete
                block = Block(bstart, d, bfiles)
                self.blocks.append(block)
                newblock = True
            d += oneday

    def histogram(self, cmd):
        b = int(cmd[1:]) if len(cmd) > 1 else None
        if b is None:
            hstart = self.start
            hend = self.end
            block = None
        else:
            # Use block number, but pad by a few days
            block = self.blocks[b]
            hstart = max((block.start - 5*oneday), self.start)
            hend = min((block.end + 5*oneday), self.end)
        d = hstart
        while (d <= hend):
            if block is not None and d == block.start:
                print("Block start")
            print(f"{d.isoformat()}: {'*'*len(self.bydate[d])}")
            if block is not None and d == block.end:
                print("Block end")
            d += oneday




if __name__ == "__main__":
    # Option Parsing
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--demon", action="store_true", dest="demon",
                        help="Run in the background, do not generate stdout")

    parser.add_argument("--logsuffix", action="store", type=str,
                        dest="logsuffix", default=None,
                        help="Extra suffix to add on logfile")

    parser.add_argument("--interactive", action="store_true",
                        help="run in interactive mode")

    #parser.add_argument("--configfile", action="store", type=str,
    #                    help="Config file to use")

    #parser.add_argument("--section", action="store", type=str,
    #                    help="Config file section to run, usually "
    #                         "yyyymmdd-YYYYMMDD")

    #parser.add_argument("--outfile", action="store", type=str,
    #                    help="File to append output to")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Check Log Suffix
    if options.logsuffix:
        setlogfilesuffix(options.logsuffix)

    # Announce startup
    logger.info("*** generate_reduce_list_interactive.py - starting up at {}"
                .format(datetime.datetime.now()))

    # Prototyping
    start = datetime.date(2025, 1, 1)
    end = datetime.date(2025, 1, 17)
    sel = 'GMOS-N/BIAS/Raw/RAW/Pass/notengineering/notscience_verification/low/slow/fullframe/1x1'

    bl = BlockList(sel, start, end, 20)

    if options.interactive:
        cmd = 'p'
        while cmd != 'q':
            if cmd == '?':
                print("s+N: increment start of block N by 1 day\n"
                      "s-N: decrement start of block N by 1 day\n"
                      "e+N: increment start of block N by 1 day\n"
                      "E-N: decrement end of block N and start and end of all subsequent blocks by 1 day\n")
            elif cmd == 'p':
                bl.print()
            elif cmd == 'w':
                print("Write output not implemented yet")
            elif cmd.startswith(('s', 'e', 'E')) and len(cmd) > 2:
                bl.updateblock(cmd)
                bl.print()
            elif cmd.startswith('h'):
                bl.histogram(cmd)
            else:
                print("Command not understood")
            cmd = input("[q]uit [?] [p]rint [w]rite [s|e|E][+|-]NUMBER > ")



    logger.info("*** generate_reduce_list.py exiting normally at %s",
                datetime.datetime.now())
