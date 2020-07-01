import pyinotify
import re
import datetime

from fits_storage.orm import session_scope
from fits_storage.fits_storage_config import storage_root
from fits_storage.logger import logger, setdemon, setdebug
from fits_storage.utils.ingestqueue import IngestQueueUtil


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Do not actually add to ingest queue")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    inotify_ingest_queue.py - starting up at %s" % datetime.datetime.now())
    logger.info("Ingesting files from: %s" % storage_root)

    # Create the pyinotify watch manager
    wm = pyinotify.WatchManager()

    # Create the event mask
    mask = pyinotify.IN_MOVED_FROM | pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO

    # Create the Event Handler
    class HandleEvents(pyinotify.ProcessEvent):
        tmpre = re.compile('(tmp)|(swp)|(^\.)')
        def __init__(self, session, logger):
            super(HandleEvents, self).__init__()
            self.s = session
            self.l = logger
        def process_default(self, event):
            self.l.debug("Pyinotify Event: %s" % str(event))
            # Does it have a tmp or swp in the filename or start with a dot?
            if(self.tmpre.search(event.name)):
                # It's a tmp file, ignore it
                self.l.debug("Ignoring Event on tmp file: %s" % event.name)
            else:
                # Go ahead and process it
                self.l.info("Processing PyInotify Event on pathname: %s" % event.pathname)
                if(options.dryrun):
                    self.l.info("Dryrun mode - not actually adding to ingest queue: %s" % event.name)
                else:
                    self.l.info("Adding to Ingest Queue: %s" % event.name)
                    IngestQueueUtil(self.s, self.l).add_to_queue(event.name, '')


    with session_scope() as session:
        # Create the notifier
        notifier = pyinotify.Notifier(wm, HandleEvents(session, logger))

        # Add the watch
        wm.add_watch(storage_root, mask)

        # Go into the notifier event loop
        try:
            notifier.loop()
        finally:
            logger.info("*** inotify_ingest_queue.py exiting normally at %s" % datetime.datetime.now())
