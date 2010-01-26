import logging
import logging.handlers

import FitsStorageConfig

# Create a Logger
logger = logging.getLogger()

# This is where we set what level messages we want to log.
# This should default to INFO and be setable to debug with a command line argument
logger.setLevel(logging.DEBUG)

# Create log message handlers 
filehandler=logging.handlers.TimedRotatingFileHandler(FitsStorageConfig.fits_log_file, backupCount=10, when='midnight', interval=1)
streamhandler=logging.StreamHandler()

# Create log formatter
formatter = logging.Formatter("%(asctime)s %(module)s:%(lineno)d %(levelname)s: %(message)s")

# Add formater to handlers
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)

# Add Handlers to logger
logger.addHandler(filehandler)
logger.addHandler(streamhandler)

