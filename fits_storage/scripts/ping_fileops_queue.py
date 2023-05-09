#! /usr/bin/env python3

import sys
import datetime

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope
from fits_storage.queues.queue.fileopsqueue import FileopsQueue, \
    FileOpsRequest, FileOpsResponse

"""
This script "pings" the fileops queue, with a "response_required=True" entry.
With response_required, the fileops queue isn't like the other queues where 
you add stuff to the queue in a fire-and-forget manner. With 
response_required=True, the result gets written back to the queue entry, 
and it's up to the code that added it to the queue to read the resonse, 
check the status and delete the queue entry when complete. This script 
exercises the fileops queue with response_required by doing that with an 
"echo" command.
"""

if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug",
                      help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon",
                      help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Announce startup
    logger.info("***   ping_fileops_queue.py - starting up at %s"
                % datetime.datetime.now())


    with session_scope() as session:
        fq = FileopsQueue(session, logger=logger)

        logger.info("Adding Request to Fileops Queue")
        fo_req = FileOpsRequest("echo", {"echo": "Hello, world"})
        fqe = fq.add(fo_req, response_required=True)
        if fqe is None:
            logger.error("Could not add fileops queue entry. Exiting")
            sys.exit(1)

        logger.info("FQE id is %d. Waiting for response", fqe.id)

        response = fq.poll_for_response(fqe.id)

        if response is None:
            logger.error("Timed out waiting for Fileops response. Exiting")
            sys.exit(2)

        logger.info("Got response with ok=%s, value=%s, error=%s",
                    response.ok, response.value, response.error)