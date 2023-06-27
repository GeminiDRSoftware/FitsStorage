#!/usr/bin/env python3

import http
import sys
import datetime
import requests
from optparse import OptionParser

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.config import get_config
fsc = get_config()

# Option Parsing
parser = OptionParser()
parser.add_option("--tapeserver", action="store", type="string",
                  dest="tapeserver", default=fsc.tape_server,
                  help="The Fits Storage Tape server to check against")
parser.add_option("--file-pre", action="store", type="string", dest="filepre",
                  help="File prefix to operate on, eg N20090130, N200812 etc")
parser.add_option("--only", action="store_true", dest="only",
                  help="Only list files that need to go to tape")
parser.add_option("--notpresent", action="store_true", dest="notpresent",
                  help="Include files that are marked as not present")
parser.add_option("--mintapes", action="store", type="int", dest="mintapes",
                  default=2,
                  help="Minimum number of tapes a file must be on to be "
                       "eligible for deletion")
parser.add_option("--tapeset", action="store", type="int", dest="tapeset",
                  help="Only consider tapes in this tapeset")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


msg = ""
# Announce startup
logger.info("***   check_on_tape.py - starting up at %s",
            datetime.datetime.now())

with session_scope() as session:
    query = session.query(DiskFile).filter(DiskFile.canonical==True)

    if options.filepre:
        query = query.filter(DiskFile.filename.startswith(options.filepre))

    if not options.notpresent:
        query = query.filter(DiskFile.present==True)

    query = query.order_by(DiskFile.filename)

    cnt = query.count()
    if cnt == 0:
        logger.info("No Files found matching file-pre. Exiting")
        sys.exit(0)
    logger.info("Got %d files to check" % cnt)

    sumbytes = 0
    sumfiles = 0

    url = f"http://{options.tapeserver}/jsontapefile/{options.filepre}"
    r = requests.get(url)
    if r.status_code != http.HTTPStatus.OK:
        logger.error("Got status code %s for url %s", (r.status_code, url))
        sys.exit(1)
    filesontape = r.json()
    if len(filesontape) == 0:
        logger.error("Server returned no fies on tape, exiting")
        sys.exit(2)

    # There might be a more optimized way to do this...
    for diskfile in query:
        fullpath = diskfile.fullpath
        dbfilename = diskfile.filename.rstrip(".bz2")
        dbmd5 = diskfile.data_md5

        tapeids = set()
        for d in filesontape:
            if d['filename'].rstrip(".bz2") == dbfilename \
                    and d['data_md5'] == dbmd5:
                if options.tapeset and options.tapeset != d['tape_set']:
                    continue

                tapeids.add(d['tape_id'])

        if len(tapeids) < options.mintapes:
            sumbytes += diskfile.file_size
            sumfiles += 1
            logger.info("File %s - %s needs to go to tape, it is only on %d "
                        "tapes: %s" % (fullpath, dbmd5, len(tapeids), tapeids))
        else:
            if not options.only:
                logger.info("File %s - %s is OK, it already is on %d tapes: %s"
                            % (fullpath, dbmd5, len(tapeids), tapeids))

    logger.info("Found %d files totalling %.2f GB that should go to tape"
                % (sumfiles, sumbytes/1.0E9))
    logger.info("*** check_on_tape.py exiting normally at %s",
                datetime.datetime.now())
