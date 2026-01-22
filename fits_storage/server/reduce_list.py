# This module contains utility code to support the generate_reduce_list.py
# script.

import datetime
import configparser
import itertools
import os.path
import sys
from ast import literal_eval
import requests

from fits_storage.config import get_config
from fits_storage.logger_dummy import DummyLogger


class ReduceListConfig:
    # Utility class for parsing the reduce list config files.

    def __init__(self, configfile, logger=DummyLogger()):
        self.fsc = get_config()
        self.l = logger

        self.cp = configparser.ConfigParser()
        self.config = {}

        self._read(configfile)

        # Convert <ConfigParser> 'cp' to actual dictionary 'config' so that we
        # can store things like datetime.dates in it. We never write it back
        # to a file anyway
        self._convert()

        # Parse the startdate and enddate from the section name and add it to
        # each section dictionary
        self._parse_section_names()

        # Determine dateranges - loop through each section, creating a
        # dateranges list of (start, end) pairs, trying to divide the section
        # into equal-ish chunks according to the ndays or mindays / maxdays
        # defined in that section.
        self._determine_dateranges()

    def _read(self, configfile):
        configpath = self.fsc.generate_reduce_list_configdir
        if not configpath:
            self.l.error("No generate_reduce_list_configdir defined in config. "
                         "Exiting...")
            exit(1)

        if not configfile:
            self.l.error("No configfile specified, exiting.")
            exit(1)

        fpfn = os.path.join(configpath, configfile)
        self.l.debug(f"Reading config file {fpfn}")

        if not os.path.exists(fpfn):
            self.l.error(f"Config file {fpfn} does not exist. Exiting")
            sys.exit(1)

        self.cp.read(fpfn)

    def _convert(self):
        for section in self.cp.sections():
            self.config[section] = {}
            for key, val in self.cp.items(section):
                self.config[section][key] = val

    def _parse_section_names(self):
        for sectname in self.config.keys():
            try:
                start_string, end_string = sectname.split('-')
                sectstart = datetime.date.fromisoformat(start_string)
                sectend = datetime.date.fromisoformat(end_string)
                self.l.debug(f"Config section {sectname}: {sectstart} - {sectend}")
                self.config[sectname]['startdate'] = sectstart
                self.config[sectname]['enddate'] = sectend
            except Exception:
                self.l.error(f'Failed to parse config section name {sectname}. '
                             f'Ignoring')

    def _determine_ndays(self, section):
        enddate = self.config[section]['enddate']
        startdate = self.config[section]['startdate']
        ndays = int(self.config[section]['ndays'])

        totaldays = (enddate - startdate).days + 1 # range is inclusive

        if totaldays % ndays == 0:
            return ndays

        mindays = int(self.config[section]['mindays'])
        maxdays = int(self.config[section]['maxdays'])
        # make corresponding ndays and remainder lists, from maxdays to mindays
        ndayslist = []
        remainders = []
        i = maxdays
        while i >= mindays:
            ndayslist.append(i)
            remainders.append(totaldays % i)
        remainder = totaldays % i
        bestndays = ndayslist.index(remainder)
        # bestndays is now the best value, but there will be 'remainder' extra days

        nblocks = totaldays // bestndays

        bestndays = [bestndays] * nblocks
        for i in range(remainder):
            bestndays[i] += 1

        return bestndays

    def _determine_dateranges(self):
        for section in self.config.keys():
            pass

    def instconfs(self, sectname):
        # Iterate through the relevant instrument configs, return a list of
        # selection strings formed by appending each instrument config to
        # the 'base' selection.
        try:
            insconf_string = self.config[sectname]['instconfs']
            iterable_names = literal_eval(insconf_string)
            self.l.debug(f"{iterable_names=}")

            iterables = [literal_eval(self.config[sectname][i])
                         for i in iterable_names]
            self.l.debug(f"{iterables=}")
        except KeyError:
            self.l.debug("No instconf defined")
            iterables = []
        selections = []
        base = self.config[sectname]['base']
        if iterables:
            for ic in itertools.product(*iterables):
                selection = f"{base}/{'/'.join(ic)}"
                self.l.debug(f"{selection=}")
                selections.append(selection)
        else:
            selections = [base]
        return selections

def findfiles(selection, start, end, logger=DummyLogger()):
    baseurl = "https://archive.gemini.edu/jsonfilelist/canonical/"
    daterange = f"{start.strftime('%Y%m%d')}" if end is None else \
        f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    logger.debug(f"FindFiles Daterange: {daterange}; Selection: {selection}")

    selection = f"/{daterange}" if selection is None else f"{selection}/{daterange}"

    url = baseurl + '/' + selection
    logger.debug(f"Fetching {url}")
    r = requests.get(url)

    if r.status_code != 200:
        logger.error(f"Got status code {r.status_code} fetching {url}")
        return []

    jfl = r.json()
    filenames = []
    for jf in jfl:
        if jf['path']:
            filenames.append(f"{jf['path']}/{jf['filename']}")
        else:
            filenames.append(jf['filename'])

    logger.debug(f"findfiles found {len(filenames)} files")

    return filenames

def parse_listfile(fp):
    # Parse a list of lists file. One list per line. Ignore blank lines and
    # comment lines. Lists can be comma and/or space separated.
    # Pass an open file pointer. We will return a list of lists.
    lists = []
    for line in fp:
        line = line.strip()
        if line.startswith('#') or len(line) == 0:
            continue
        line = line.replace(',', ' ')
        files = line.split()
        lists.append(files)
    return lists
