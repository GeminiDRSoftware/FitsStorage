"""
This is the fits storage logging module
It is used by the scripts to configure the python logger
"""

import os
import sys

import logging
import logging.handlers

from .fits_storage_config import fits_log_dir, email_errors_to, smtp_server

# Create a Logger
logger = logging.getLogger()

# This is where we set what level messages we want to log.
# Default to INFO and be setable to debug with a command line argument
logger.setLevel(logging.DEBUG)

# Create log formatter
formatter = logging.Formatter("%(asctime)s %(process)d:%(module)s:%(lineno)d %(levelname)s: %(message)s")

# Create log message handlers
# Set default logname
logname = "%s.log" % (os.path.basename(sys.argv[0]))
logfile = os.path.join(fits_log_dir, logname)
filehandler = logging.handlers.RotatingFileHandler(logfile, backupCount=10, maxBytes=10000000)
streamhandler = logging.StreamHandler()
emailsubject = "Messages from FitsStorage on %s" % os.uname()[1]
smtphandler = logging.handlers.SMTPHandler(mailhost=smtp_server, fromaddr='fitsdata@gemini.edu',
                    toaddrs=[email_errors_to], subject=emailsubject)

# The smtp handler should only do CRITICALs or worse
smtphandler.setLevel(logging.CRITICAL)

# Add formater to handlers
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)
smtphandler.setFormatter(formatter)

# Add Handlers to logger
#logger.addHandler(filehandler)
sysloghandler = logging.handlers.SysLogHandler()
sysloghandler.setFormatter(formatter)
#logger.addHandler(sysloghandler)
logger.addHandler(streamhandler)

# Turn off boto debug logging
# We do this inside utils.aws_s3 now
# logging.getLogger('boto').setLevel(logging.WARNING)

# Utility functions follow

# env var setting for webserver
loglevels = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARN}
loglevel = os.getenv("LOG_LEVEL", None)
if loglevel is not None:
    if loglevel in loglevels:
        logger.setLevel(loglevels[loglevel])


def setdebug(want):
    """ Set if we want debug messages """
    if want:
        logger.setLevel(logging.DEBUG)

def setdemon(want):
    """ If running as a demon, don't output to stdout but do activate email handler """
    return
    if want:
        if len(email_errors_to):
            logger.addHandler(smtphandler)
    else:
        logger.addHandler(streamhandler)

def setlogfilesuffix(suffix):
    """ Set a suffix on the log file name """
    return
    logname = "%s-%s.log" % (os.path.basename(sys.argv[0]), suffix)
    logfile = os.path.join(fits_log_dir, logname)
    new_filehandler = logging.handlers.RotatingFileHandler(logfile, backupCount=10, maxBytes=10000000)
    new_filehandler.setFormatter(formatter)
    logger.removeHandler(filehandler)
    logger.addHandler(new_filehandler)
