import os
import sys

import logging
import logging.handlers

from fits_storage_config import fits_log_dir, email_errors_to, smtp_server

# Create a Logger
logger = logging.getLogger()

# This is where we set what level messages we want to log.
# This should default to INFO and be setable to debug with a command line argument
logger.setLevel(logging.INFO)

# Create log formatter
formatter = logging.Formatter("%(asctime)s %(process)d:%(module)s:%(lineno)d %(levelname)s: %(message)s")

# Create log message handlers 

# Set default logname
logname = "%s.log" % (os.path.basename(sys.argv[0]))
logfile = os.path.join(fits_log_dir, logname)
filehandler = logging.handlers.RotatingFileHandler(logfile, backupCount=10, maxBytes=10000000)
streamhandler = logging.StreamHandler()
emailsubject = "Messages from FitsStorage on %s" % os.uname()[1]
smtphandler = logging.handlers.SMTPHandler(mailhost=smtp_server, fromaddr='fitsdata@gemini.edu', toaddrs=[email_errors_to], subject=emailsubject)

# The smtp handler should only do WARNINGSs or worse
smtphandler.setLevel(logging.WARNING)

# Add formater to handlers
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)
smtphandler.setFormatter(formatter)

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
        if(len(email_errors_to)):
            logger.addHandler(smtphandler)
    else:
        logger.addHandler(streamhandler)

def setlogfilesuffix(suffix):
    logname = "%s-%s.log" % (os.path.basename(sys.argv[0]), suffix)
    logfile = os.path.join(fits_log_dir, logname)
    new_filehandler = logging.handlers.RotatingFileHandler(logfile, backupCount=10, maxBytes=10000000)
    new_filehandler.setFormatter(formatter)
    logger.removeHandler(filehandler)
    logger.addHandler(new_filehandler)
