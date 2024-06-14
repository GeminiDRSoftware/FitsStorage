#!/usr/bin/env python3

from optparse import OptionParser
import os
import smtplib
import json

from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.config import get_config

parser = OptionParser()
parser.add_option("--email-to", action="store", dest="emailto",
                  help="Send Email report to this address")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

fsc = get_config()

if not fsc.diskspace_check:
    logger.error("diskspace_check is not defined in config. Exiting")
    exit()


# A shortcut to log messages and build an email message.
def domsg(string):
    global message
    message += string
    message += '\n'
    logger.info(string)


try:
    disks = json.loads(fsc.diskspace_check)
except json.decoder.JSONDecodeError:
    logger.debug("Error parsing JSON: %s", fsc.diskspace_check)
    disks = None

message = "Diskspace Check script messages:\n\n"
if disks is None:
    domsg("Unable to parse JSON diskspace_check configuration string.")

low = 0
for disk in disks.keys():
    # Do a chdir to kick the automounter
    os.chdir(disk)
    s = os.statvfs(disk)
    gbavail = s.f_bsize * s.f_bavail / (1024 * 1024 * 1024)
    if gbavail < disks[disk]:
        low += 1
        domsg("Disk %s is LOW ON SPACE - Free space = %.2f GB, should be at "
              "least %.2f GB\n\n" % (disk, gbavail, disks[disk]))
    else:
        domsg("Disk %38s is fine: %.2f GB free\n\n" % (disk, gbavail))

if options.emailto:
    subject = "Urgent: LOW DISK SPACE" if low else "Diskspace check OK"
    mailto = [options.emailto]
    msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % \
          (fsc.email_from, ", ".join(mailto), subject, message)

    server = smtplib.SMTP(fsc.smtp_server)
    server.sendmail(fsc.email_from, mailto, msg)
    server.quit()
