#! /usr/bin/env python3

import datetime

from fits_storage.config import get_config
fsc = get_config()

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix

from fits_storage.server.reduce_list import ReduceListConfig, findfiles

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

    parser.add_argument("--configfile", action="store", type=str,
                        help="Config file to use")

    parser.add_argument("--section", action="store", type=str,
                        help="Config file section to run, usually "
                             "yyyymmdd-YYYYMMDD")

    parser.add_argument("--outfile", action="store", type=str,
                        help="File to append output to")

    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Check Log Suffix
    if options.logsuffix:
        setlogfilesuffix(options.logsuffix)

    # Announce startup
    logger.info("*** generate_reduce_list.py - starting up at {}"
                .format(datetime.datetime.now()))
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    rlconfig = ReduceListConfig(options.configfile, logger=logger)

    fp = None
    if options.outfile:
        logger.info(f"Writing output lists to {options.outfile}")
        try:
            fp = open(options.outfile, "a")
        except Exception:
            logger.error(f"Failed to open outfile {options.outfile}",
                         exc_info=True)
            exit(5)

    if options.section:
        sections = [options.section]
    else:
        sections = rlconfig.config.keys()
    logger.debug(f"{sections=}")

    for section in sections:
        # Load values from the appropriate config section
        config = rlconfig.config[section]
        selections = rlconfig.instconfs(section)

        # Make the configed values ints
        ndays = int(config['ndays'])
        stepdays = int(config['stepdays']) if config['stepdays'] else ndays
        minnum = int(config['min'])
        maxnum = int(config['max'])

        # Make ndays and stepdays timedeltas.
        # Correct for the fact that the ranges are inclusive here
        ndays = datetime.timedelta(days=ndays-1)
        stepdays = datetime.timedelta(days=stepdays)

        logger.info(f"{section=} ndays={ndays.days} stepdays={stepdays.days}")

        for selection in selections:
            start = config['startdate']
            while start < config['enddate']:
                end =  min(start + ndays, config['enddate'])
                actual_ndays = (end-start).days + 1
                logger.debug(f"{selection=}, {start=}, {end=}")

                filenames = findfiles(selection, start, end, logger=logger)
                if len(filenames) < minnum:
                    logger.warning("Failed to find sufficient files for "
                                   f"instrument config {selection}")

                if len(filenames) > maxnum:
                    logger.warning("Got more files than maxnum, truncating list")
                    filenames = filenames[:maxnum]
                if filenames:
                    logger.info(f"{start} - {end} [{actual_ndays} days], "
                                f"{selection}: "
                                f"{len(filenames)} - {filenames[0]}...")
                    if fp:
                        if config.group:
                            fp.write(f"# {start} - {end} [{actual_ndays} days] grouped, "
                                     f"{selection}: {len(filenames)} files\n")
                            fp.write(' '.join(filenames))
                            fp.write('\n\n')
                        else:
                            fp.write(f"# {start} - {end} [{actual_ndays} days] "
                                     f"{selection}: {len(filenames)} files\n")
                            fp.write('\n'.join(filenames))
                            fp.write('\n')
                start += stepdays

    if fp:
        fp.close()

    logger.info("*** generate_reduce_list.py exiting normally at %s",
                datetime.datetime.now())
