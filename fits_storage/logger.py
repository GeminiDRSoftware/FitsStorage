"""
This is the fits storage logging module
It is used by the scripts to configure the python logger
"""

import os
import sys

import logging
import logging.handlers

from fits_storage.config import get_config
fsc = get_config()

# Create a Logger
logger = logging.getLogger()

# This is where we set what level messages we want to log.
# Default to INFO and be setable to debug with a command line argument
logger.setLevel(logging.INFO)

# Create log formatter
formatter = logging.Formatter("%(asctime)s %(process)d:%(module)s:%(lineno)d "
                              "%(levelname)s: %(message)s")

# Create log message handlers
# Set default logname
logname = "%s.log" % (os.path.basename(sys.argv[0]))
logfile = os.path.join(fsc.log_dir, logname)
filehandler = logging.handlers.RotatingFileHandler(logfile, backupCount=10,
                                                   maxBytes=10000000)
streamhandler = logging.StreamHandler()
emailsubject = "Messages from FitsStorage on %s" % os.uname()[1]
smtphandler = logging.handlers.SMTPHandler(mailhost=fsc.smtp_server,
                                           fromaddr='fitsdata@gemini.edu',
                                           toaddrs=[fsc.email_errors_to],
                                           subject=emailsubject)

# The smtp handler should only do CRITICAL or worse
smtphandler.setLevel(logging.CRITICAL)

# Add formatter to handlers
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)
smtphandler.setFormatter(formatter)

# Add Handlers to logger
if fsc.log_dir != '':
    logger.addHandler(filehandler)


# Utility functions follow

# env var setting for webserver
loglevels = {"DEBUG": logging.DEBUG, "INFO": logging.INFO,
             "WARNING": logging.WARN}
loglevel = os.getenv("LOG_LEVEL", None)
if loglevel is not None:
    if loglevel in loglevels:
        logger.setLevel(loglevels[loglevel])


def setdebug(want):
    """ Set if we want debug messages """
    if want:
        logger.setLevel(logging.DEBUG)


def setdemon(want):
    """
    If running as a demon, don't output to stdout but do activate email
    handler
    """
    if want:
        if fsc.email_errors_to != '':
            logger.addHandler(smtphandler)
    else:
        logger.addHandler(streamhandler)


def setlogfilesuffix(suffix):
    """ Set a suffix on the log file name """
    logname = "%s-%s.log" % (os.path.basename(sys.argv[0]), suffix)
    logfile = os.path.join(fsc.log_dir, logname)
    new_filehandler = logging.handlers.RotatingFileHandler(logfile,
                                                           backupCount=10,
                                                           maxBytes=10000000)
    new_filehandler.setFormatter(formatter)
    logger.removeHandler(filehandler)
    logger.addHandler(new_filehandler)


class DummyLogger(object):
    """
    A dummy object that you can treat as a logger but which
    does absolutely nothing.
    """
    def noop(self, *stuff):
        pass
    info = noop
    error = noop
    debug = noop
    warning = noop
