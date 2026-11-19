#! /usr/bin/env python3
"""
Update headers on a FitsStorage server via the update_headers API.

Files are selected using a standard FitsStorage selection string, which is
passed to the server's /jsonsummary/present/<selection> endpoint. Without
--update, the matching files are just listed. With --update, the requested
changes are sent to the server's /update_headers endpoint in batches.

Note that a successful response from the server means the update was
*queued* on the server's fileops queue, not that it has been applied yet.

Exit codes:
    0 - success (or nothing to do)
    1 - invalid command line arguments
    2 - invalid release date
    3 - --update given without anything to update
    4 - GEMINI_API_AUTHORIZATION not set
    5 - error talking to the server while sending updates
    6 - error getting the file list, or refusing a truncated file list
    7 - the server rejected one or more of the updates
"""

import datetime
import os
import sys

from argparse import ArgumentParser

import requests

from fits_storage.logger import logger, setdebug, setdemon

QA_STATES = ('pass', 'fail', 'usable', 'undefined', 'check')
DEFAULT_BATCH_SIZE = 200
TIMEOUT = 300  # seconds, for each HTTP request


def main():
    parser = ArgumentParser(prog='server_update_headers.py',
                            description='Update Headers on fits server via '
                                        'API. TODO: More actions need adding.')

    parser.add_argument("--server", action="store", dest="server",
                        help="FitsStorage Server to use, "
                             "eg https://archive.gemini.edu")
    parser.add_argument("--debug", action="store_true", dest="debug",
                        default=False, help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true", dest="demon",
                        default=False, help="Do not log to stdout")
    parser.add_argument("--selection", action="store", dest="selection",
                        help="selection string for files to update")
    parser.add_argument("--update", action="store_true",
                        help="update headers. Without this just list files "
                             "that match")
    parser.add_argument("--release", action="store",
                        help="update release date. Format: YYYY-MM-DD")
    parser.add_argument("--qastate", action="store", type=str.lower,
                        choices=QA_STATES,
                        help="update qa state to this value "
                             "(case insensitive)")
    parser.add_argument("--batch-size", action="store", type=int,
                        dest="batch_size", default=DEFAULT_BATCH_SIZE,
                        help="Number of files to send per update request. "
                             f"Default {DEFAULT_BATCH_SIZE}")
    options = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("***   server_update_headers.py - starting up at "
                f"{datetime.datetime.now()}")

    if options.selection is None or options.server is None:
        logger.error("Must specify a server and a selection")
        return 1

    options.server = options.server.rstrip('/')
    if not options.server.startswith(('http://', 'https://')):
        logger.error(f"Server must start with http:// or https:// - "
                     f"got {options.server}")
        return 1

    if options.batch_size < 1:
        logger.error(f"Invalid batch size: {options.batch_size}")
        return 1

    if options.release:
        try:
            # Same format check as the server uses (FitsEditor.set_release)
            release = datetime.datetime.strptime(options.release,
                                                 '%Y-%m-%d').date()
            if not 2000 <= release.year < 2100:
                raise ValueError("Year out of range")
        except ValueError:
            logger.error(f"Invalid release date: {options.release}. "
                         "Exiting with no updates sent")
            return 2
        # Normalize, eg 2025-1-5 -> 2025-01-05
        options.release = release.isoformat()

    if options.update:
        if not (options.release or options.qastate):
            logger.error("--update given but nothing to update. Specify "
                         "--release and/or --qastate. Exiting with no "
                         "updates sent")
            return 3
        if os.environ.get('GEMINI_API_AUTHORIZATION') is None:
            logger.error("GEMINI_API_AUTHORIZATION not set. Exiting with no "
                         "updates sent")
            return 4

    return server_update_headers(options, logger)


def server_update_headers(options, logger):
    files, truncated = get_file_list(options.server, options.selection,
                                     logger)
    if files is None:
        return 6

    if truncated:
        msg = (f"The selection '{options.selection}' hit the server's "
               f"result limit of {len(files)} files, so the results are "
               "probably truncated. Narrow the selection (eg add or shorten "
               "a date range, or add a program ID or observation ID).")
        if options.update:
            logger.error(msg + " Exiting with no updates sent.")
            return 6
        logger.warning(msg)

    if not files:
        logger.info("No files match the selection. Nothing to do.")
        return 0

    if not options.update:
        logger.info("List only. Use --update to apply updates.")
        return 0

    cookie = os.environ['GEMINI_API_AUTHORIZATION']
    return apply_updates(files, options, cookie, logger)


def get_file_list(server, selection, logger):
    """
    Get the list of filenames matching the selection from the server.
    Returns (filenames, truncated). filenames is None on error.
    """
    url = f"{server}/jsonsummary/present/{selection}"
    logger.debug(f"Getting file list from {url}")

    # See if the user has an archive session key to use
    key = get_goa_session_key()
    cookies = {'gemini_archive_session': key} if key else {}

    try:
        r = requests.get(url, cookies=cookies, timeout=TIMEOUT)
    except requests.RequestException as e:
        logger.error(f"Error connecting to {server}: {e}")
        return None, False

    if r.status_code != 200:
        logger.error(f"Error status code from {server}: {r.status_code} - "
                     f"{r.text[:500]}")
        return None, False

    try:
        results = r.json()
    except ValueError:
        logger.error(f"Could not decode file list from {server}: "
                     f"{r.text[:500]}")
        return None, False

    truncated = bool(results) and \
        bool(results[-1].get('results_truncated', False))

    filenames = []
    for i in results:
        logger.info(f"Got file: {i['filename']} - {i['data_label']}")
        filenames.append(i['filename'])

    if filenames:
        logger.info(f"- Got {len(filenames)} files matching selection: "
                    f"{filenames[0]} ... {filenames[-1]}")
    return filenames, truncated


def apply_updates(files, options, cookie, logger):
    """
    Send the update requests to the server, in batches. The same update is
    applied to all the files. Returns an exit status code.

    The request is a json list of dictionaries, one per file. Each dict
    looks like: {'filename': string, 'values': dict, 'reject_new': False}
    and the values dict looks like: {'qa_state': 'pass',
    'release': 'YYYY-MM-DD'}
    """
    values = {}
    if options.qastate:
        values['qa_state'] = options.qastate
    if options.release:
        values['release'] = options.release

    url = f"{options.server}/update_headers"
    cookies = {'gemini_api_authorization': cookie}
    batch_size = options.batch_size

    logger.info(f"Sending update {values} for {len(files)} files to {url} "
                f"in batches of up to {batch_size}")

    n_ok = 0
    n_fail = 0
    for start in range(0, len(files), batch_size):
        batch = files[start:start+batch_size]
        request = [{'filename': f, 'values': dict(values),
                    'reject_new': False} for f in batch]
        logger.debug(f"Sending batch of {len(batch)} files, starting at "
                     f"{batch[0]}")

        try:
            r = requests.post(url, cookies=cookies, json=request,
                              timeout=TIMEOUT)
        except requests.RequestException as e:
            logger.error(f"Error connecting to {options.server}: {e}")
            return _abort(n_ok, n_fail, len(files), logger)

        if r.status_code != 200:
            logger.error(f"Got bad status code {r.status_code}: "
                         f"{r.text[:500]}")
            return _abort(n_ok, n_fail, len(files), logger)
        logger.debug("Got HTTPOK status.")

        try:
            response = r.json()
        except ValueError:
            logger.error(f"Could not decode response: {r.text[:500]}")
            return _abort(n_ok, n_fail, len(files), logger)

        if not isinstance(response, list):
            logger.error(f"Got unexpected response: {response}")
            return _abort(n_ok, n_fail, len(files), logger)

        if len(response) != len(batch):
            logger.warning(f"Sent {len(batch)} updates but got "
                           f"{len(response)} responses")

        for item in response:
            if item.get('result'):
                n_ok += 1
                logger.info(f"{item.get('id')}: queued")
            else:
                n_fail += 1
                logger.error(f"{item.get('id', '(no id)')}: "
                             f"{item.get('error', 'unknown error')}")

    logger.info(f"Done. {n_ok} updates queued, {n_fail} rejected. "
                "Queued updates are applied asynchronously by the server.")
    return 7 if n_fail else 0


def _abort(n_ok, n_fail, n_total, logger):
    logger.error(f"Stopped part way through: {n_ok} updates queued, "
                 f"{n_fail} rejected, {n_total - n_ok - n_fail} not sent.")
    return 5


def get_goa_session_key():
    # Look for ~/.gemini_archive_session
    p = os.path.expanduser('~/.gemini_archive_session')
    k = None
    try:
        with open(p) as f:
            k = f.readline().strip()
    except IOError:
        pass
    return k


if __name__ == '__main__':
    sys.exit(main())
