#! /usr/bin/env python3

import datetime
import signal
import time
import os

from argparse import ArgumentParser

from fits_storage.logger import logger, setdebug, setdemon, setlogfilesuffix
from fits_storage.server.pidfile import PidFile, PidFileError

from fits_storage.db import sessionfactory
from fits_storage.queues.queue.reducequeue import ReduceQueue

# This is done later - see below for why
# from fits_storage.server.reducer import Reducer, ReducerMemoryLeak

from fits_storage.config import get_config
fsc = get_config()

# Reduce() ETI gets really messed up if we don't have the __name__ idiom:
if __name__ == "__main__":
    parser = ArgumentParser(prog='service_reduce_queue.py',
                            description='Service the FitsStorage Reduce Queue')

    parser.add_argument("--nocleanup", action="store_true", dest="nocleanup",
                        default=False, help="Do not clean up working "
                                            "directories")
    parser.add_argument("--debug", action="store_true", dest="debug",
                        default=False, help="Increase log level to debug")
    parser.add_argument("--demon", action="store_true", dest="demon",
                        default=False,
                        help="Run as background demon, do not generate stdout")
    parser.add_argument("--name", action="store", dest="name",
                        help="Name for this instance of this task. "
                             "Used in logfile and lockfile")
    parser.add_argument("--lockfile", action="store_true", dest="lockfile",
                        help="Use a lockfile to limit instances")
    parser.add_argument("--empty", action="store_true", dest="empty", default=False,
                        help="Exit once the queue is empty.")
    parser.add_argument("--oneshot", action="store_true", dest="oneshot",
                        default=False, help="Process only one file then exit")
    options = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    if options.name is not None:
        setlogfilesuffix(options.name)

    # Some primitives make (indirect) use of numpy.linalg eg .lstsq which uses
    # BLAS or similar, which will by default use many threads / CPU cores for
    # speed. This can be an issue when we are running many instances of Reduce() on
    # the same machine - if each one tries to use all the cores, the system grinds
    # to a halt.  Since most of the processing is single threaded, we generally run
    # as many processing jobs as we have cores, and thus it's preferable to have
    # each job stay single threaded, which is done by setting the OMP_NUM_THREADS
    # (and others? it's unclear...) environment variable to 1. We store this in the
    # reduce_linalg_threads configuration parameter. We only set the environment
    # variable if it not already set.

    # Irritatingly, this seems to need to be done before the relevant import
    if fsc.reduce_linalg_threads is not None:
        if 'OMP_NUM_THREADS' not in os.environ:
            logger.info(f"Setting [OMP|OPENBLAS|MKL|BLIS]_NUM_THREADS to {str(fsc.reduce_linalg_threads)}")
            for i in ['OMP', 'OPENBLAS', 'MKL', 'BLIS']:
                os.environ[f'{i}_NUM_THREADS'] = str(fsc.reduce_linalg_threads)
        else:
            logger.warning(f"OMP_NUM_THREADS={os.environ['OMP_NUM_THREADS']} "
                           f"already set in environment. Not over-riding.")

    # *NOW* we can import Reducer, which imports the DRAGONS API...
    from fits_storage.server.reducer import Reducer, ReducerMemoryLeak

    # Need to set up the global loop variable before we define the signal handlers
    loop = True

    # Define signal handlers. This allows us to bail out cleanly e.g. if we get
    # a signal. These need to be defined after logger is set up as there is no
    # way to pass the logger as an argument to these.
    def handler(signum, frame):
        logger.error("Received signal: %d. Crashing out.", signum)
        raise KeyboardInterrupt('Signal', signum)


    def nicehandler(signum, frame):
        logger.error("Received signal: %d. Attempting to stop nicely.", signum)
        global loop
        loop = False


    # Set handlers for the signals we want to handle
    # Cannot trap SIGKILL or SIGSTOP, all others are fair game
    # Don't trap SIGPIPE - if that happens, we want to see the exception.
    signal.signal(signal.SIGHUP, nicehandler)
    signal.signal(signal.SIGINT, nicehandler)
    signal.signal(signal.SIGQUIT, nicehandler)
    signal.signal(signal.SIGILL, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGFPE, handler)
    signal.signal(signal.SIGSEGV, handler)
    signal.signal(signal.SIGTERM, nicehandler)

    # Announce startup
    logger.info("***   service_reduce_queue.py - starting up at %s",
                datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    try:
        with PidFile(logger, options.name, dummy=not options.lockfile) as pidfile:
            session = sessionfactory()

            reduce_queue = ReduceQueue(session, logger)

            # Loop forever. loop is a global variable defined up top
            while loop:
                try:
                    # Request a queue entry. The returned entry is marked
                    # as inprogress and committed to the session.
                    rqe = reduce_queue.pop(logger=logger)

                    # There's a subtle gotcha in that with sqlalchemy, almost
                    # any access to the data values of an ORM instance will
                    # start a transaction and until that transaction ends
                    # (ie COMMIT;s) we cannot get an ACCESS EXCLUSIVE lock to
                    # pop another queue entruy (in any process). So in all the
                    # reducequeue and reducer code, we need to be diligent about
                    # doing a session.commit() after we are done accessing
                    # the reducequeue instance, even if we didn't modify it.

                    if rqe is None:
                        if options.empty:
                            logger.info("Nothing on queue and "
                                        "--empty flag set, exiting")
                            break
                        else:
                            logger.info("Nothing on queue... Waiting")
                            time.sleep(2)
                            continue

                    if options.oneshot:
                        loop = False

                    logger.info("Reducing rqe id %s - %s... [%d] - (%d on "
                                "queue)" % (rqe.id, rqe.filenames[0],
                                            len(rqe.filenames),
                                            reduce_queue.length()))
                    session.commit()  # See note above

                    # Go ahead and initiate reduction. At this point, rqe is
                    # marked as inprogress and is committed to the database.
                    # The reducer should handle everything from here -
                    # including deleting the rqe if it successful and
                    # setting the status and error messages in rqe and
                    # outputting appropriate log messages if there's a failure.

                    reducer = Reducer(session, logger, rqe,
                                      nocleanup=options.nocleanup)
                    reducer.do_reduction()

                except ReducerMemoryLeak:
                    logger.error("ReducerMemoryLeak detected - exiting gracefully")
                    loop = False

                except KeyboardInterrupt:
                    logger.error("KeyboardInterrupt - exiting ungracefully!")
                    loop = False
                    break

                except:
                    # reducer should handle its own exceptions, and
                    # should never raise, so if there's a problem with a given
                    # file we log the error and continue with the next one.
                    # This catches anything else in the code above. Again, we
                    # log the error and carry on. Probably the error would
                    # reoccur if we re-try the same job though, so we set it
                    # as failed and record the error in the rqe too.

                    # Log the exception right away so that if the handling fails
                    # we still get an error message
                    logger.error("Unhandled Exception in service_reduce_queue!",
                                 exc_info=True)
                    message = "Unknown Error - no ReduceQueueEntry instance"
                    if rqe is not None:
                        try:
                            rqe.failed = True
                            rqe.inprogress = False
                            message = f"Exception in service_reduce_queue " \
                                      f"while processing id {rqe.id}"
                            rqe.error = message
                            session.commit()
                        except:
                            logger.error("Exception while trying to handle "
                                         "exception in service_reduce_queue",
                                         exc_info=True)

                    logger.error(message)
                    # This is drastic. raise the exception so we crash out.
                    # We need to figure out what causes any occurence of this.
                    raise
    except PidFileError as e:
        logger.error(str(e))

    logger.info("***    service_reduce_queue.py - exiting at %s",
                datetime.datetime.now())
