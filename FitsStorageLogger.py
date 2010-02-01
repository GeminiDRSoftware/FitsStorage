import os.path
import sys

import logging
import logging.handlers

import FitsStorageConfig

# Create a Logger
logger = logging.getLogger()

# This is where we set what level messages we want to log.
# This should default to INFO and be setable to debug with a command line argument
logger.setLevel(logging.INFO)

# Create log message handlers 
myname = "%s-%s.log" % (FitsStorageConfig.fits_installation, os.path.basename(sys.argv[0]))
logfile = os.path.join(FitsStorageConfig.fits_log_dir, myname)
filehandler=logging.handlers.TimedRotatingFileHandler(logfile, backupCount=10, when='midnight', interval=1)
streamhandler=logging.StreamHandler()

# Create log formatter
formatter = logging.Formatter("%(asctime)s %(process)d:%(module)s:%(lineno)d %(levelname)s: %(message)s")

# Add formater to handlers
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)

# Add Handlers to logger
logger.addHandler(filehandler)

# Do not add this one by default. Applications can do this if they're running online
#logger.addHandler(streamhandler)


# Utility Functions
def setdebug(want):
  if(want):
    logger.setLevel(logging.DEBUG)

def setdemon(want):
  if(want):
    pass
  else:
    logger.addHandler(streamhandler)

