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

    parser.add_argument("--daterange", action="store", type=str,
                        help="Date Range, yyyymmdd-YYYYMMDD")

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

    # Parse the start and end dates
    if options.daterange:
        try:
            start_string, end_string = options.daterange.split('-')
            start = datetime.date.fromisoformat(start_string)
            end = datetime.date.fromisoformat(end_string)
        except Exception:
            logger.error(f"Failed to parse daterange option: "
                         f"{options.daterange}, Exiting", exc_info=True)
            exit(3)
    else:
        start, end = rlconfig.startend()
    logger.info(f"Start-End dates: {start} - {end}")

    fp = None
    if options.outfile:
        logger.info(f"Writing output lists to {options.outfile}")
        try:
            fp = open(options.outfile, "a")
        except Exception:
            logger.error(f"Failed to open outfile {options.outfile}",
                         exc_info=True)
            exit(5)

    win_start = start
    while win_start <= end:

        # Load values from the appropriate config section
        selections = rlconfig.instconfs(win_start)
        ndays, stepdays, min, max = rlconfig.values(win_start)

        for selection in selections:
            allfiles = findfiles(selection, win_start, ndays, logger=logger)
            if len(allfiles) < min:
                logger.warning("Failed to find sufficient files for "
                               f"instrument config {selection}")

            filenames = allfiles[:max]
            if filenames:
                logger.info(f"{win_start} for {ndays.days} days, {selection}: "
                            f"{len(filenames)} - {filenames[0]}...")

                if fp:
                    fp.write(f"# {win_start} for {ndays.days} days, "
                             f"{selection}: {len(filenames)} files}\n")
                    fp.write(' '.join(filenames))
                    fp.write('\n\n')

        win_start += stepdays

    if fp:
        fp.close()

    logger.info("*** generate_reduce_list.py exiting normally at %s",
                datetime.datetime.now())
