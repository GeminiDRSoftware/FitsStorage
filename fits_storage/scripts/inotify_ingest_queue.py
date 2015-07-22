import pyinotify
import re
import datetime

from fits_storage.orm import sessionfactory
from fits_storage.fits_storage_config import storage_root
from fits_storage.logger import logger, setdemon, setdebug
from fits_storage.utils.ingestqueue import add_to_ingestqueue


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

session = sessionfactory()

# Create the pyinotify watch manager 
wm = pyinotify.WatchManager()

# Create the event mask
mask = pyinotify.IN_MOVED_FROM | pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO

# Create the Event Handler
class HandleEvents(pyinotify.ProcessEvent):
    tmpre = re.compile('(tmp)|(swp)|(^\.)')
    def process_default(self, event):
        logger.debug("Pyinotify Event: %s" % str(event))
        # Does it have a tmp or swp in the filename or start with a dot?
        if(self.tmpre.search(event.name)):
            # It's a tmp file, ignore it
            logger.debug("Ignoring Event on tmp file: %s" % event.name)
        else:
            # Go ahead and process it
            logger.info("Processing PyInotify Event on pathname: %s" % event.pathname)
            if(options.dryrun):
                logger.info("Dryrun mode - not actually adding to ingest queue: %s" % event.name)
            else:
                logger.info("Adding to Ingest Queue: %s" % event.name)
                add_to_ingestqueue(session, event.name, '')


# Create the notifier
notifier = pyinotify.Notifier(wm, HandleEvents())

# Add the watch
wm.add_watch(storage_root, mask)

# Go into the notifier event loop
try:
    notifier.loop()
finally:
    session.close()
    logger.info("*** inotify_ingest_queue.py exiting normally at %s" % datetime.datetime.now())


