# This module contains utility code to support the generate_reduce_list.py
# script.

import datetime
import configparser
import itertools
import os.path
from ast import literal_eval
import requests

from fits_storage.config import get_config
from fits_storage.logger_dummy import DummyLogger


class ReduceListConfig():
    # Utility class for parsing the reduce list config files.
    configsects = None

    def __init__(self, configfile, logger=DummyLogger()):
        self.fsc = get_config()
        self.l = logger

        self._parse_configsects(configfile)

    def _parse_configsects(self, configfile):
        configpath = self.fsc.generate_reduce_list_configdir
        if not configpath:
            self.l.error("No generate_reduce_list_configdir defined in config. "
                         "Exiting...")
            exit(1)

        if not configfile:
            self.l.error("No configfile specified, exiting.")
            exit(1)

        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(configpath, configfile))

        self.configsects = []
        for sectname in self.config.sections():
            try:
                start_string, end_string = sectname.split('-')
                sectstart = datetime.date.fromisoformat(start_string)
                sectend = datetime.date.fromisoformat(end_string)
            except Exception:

                self.l.error(f'Failed to parse config section name {sectname}. '
                             f'Exiting')
                exit(2)
            self.l.debug(f"Config section {sectname}: {sectstart} - {sectend}")
            self.configsects.append((sectname, sectstart, sectend))

    def startend(self):
        try:
            start = datetime.date.fromisoformat(
                self.config['DEFAULT']['startdate'])
            end = datetime.date.fromisoformat(
                self.config['DEFAULT']['enddate'])
        except Exception:
            self.l.error("Failed to parse start and/or end date from config "
                         "file. Exiting", exc_info=True)
            exit(4)
        return start, end

    def group(self):
        return self.config.getboolean('DEFAULT', 'group')

    def sectname(self, thedate):
        for (sectname, sectstart, sectend) in self.configsects:
            if thedate >= sectstart and thedate <= sectend:
                return sectname
        return 'DEFAULT'

    def values(self, thedate):
        thesectname = self.sectname(thedate)
        self.l.debug(f"Window starting {thedate}, "
                     f"config section {thesectname}")
        ndays = datetime.timedelta(
            days=self.config[thesectname].getint('ndays')-1)

        try:
            stepdays = self.config[thesectname].getint('stepdays')
        except ValueError:
            stepdays = None
        if stepdays:
            stepdays=datetime.timedelta(days=stepdays)
        else:
            stepdays = ndays

        min = self.config[thesectname].getint('min')
        max = self.config[thesectname].getint('max')

        return ndays, stepdays, min, max

    def instconfs(self, thedate):
        thesectname = self.sectname(thedate)

        # Iterate through the relevant instrument configs
        try:
            insconf_string = self.config[thesectname]['instconfs']
            iterable_names = literal_eval(insconf_string)
            self.l.debug(f"{iterable_names=}")

            iterables = [literal_eval(self.config[thesectname][i])
                         for i in iterable_names]
            self.l.debug(f"{iterables=}")
        except KeyError:
            self.l.debug("No instconf defined")
            iterables = []
        selections = []
        base = self.config[thesectname]['base']
        if iterables:
            for ic in itertools.product(*iterables):
                selection = f"{base}/{'/'.join(ic)}"
                self.l.debug(f"{selection=}")
                selections.append(selection)
        else:
            selections = [base]
        return selections

def findfiles(selection, win_start, interval, logger=DummyLogger()):
    baseurl = "https://archive.gemini.edu/jsonfilelist/canonical/notengineering/"
    win_end = win_start + interval
    daterange = f"{win_start.strftime('%Y%m%d')}-{win_end.strftime('%Y%m%d')}"
    logger.info(f"Daterange: {daterange}; Selection: {selection}")

    if selection is None:
        selection = f"/{daterange}"
    else:
        selection += f"/{daterange}"


    url = baseurl + selection
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
