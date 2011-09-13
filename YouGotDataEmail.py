from FitsStorage import *
import FitsStorageConfig
from FitsStorageUtils import *
from FitsStorageLogger import *

import urllib2
import sys
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--emailfrom", action="store", dest="fromaddr", default="fitsdata@gemini.edu", help="Email Address to send from")
parser.add_option("--replyto", action="store", dest="replyto", default="gnda@gemini.edu", help="Set a Reply-To email header")
parser.add_option("--date", action="store", dest="date", default = "today", help="Specify an alternate date to check for data from")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)


# ISG have local smtp servers set up on the fits hosts (mkofits1 and cpofits1) that relay mail to the gemini smtp
# servers without needing to authenticate. Otherwise, the gemini smtp will not relay to external addresses.

mailhost = "localhost"
cre = re.compile('\.fits')

logger.info("YouveGotDataEmail.py starting for date %s" % options.date)

# The project / email list. Get from the database
session = sessionfactory()
notifs = session.query(Notification).all()

for notif in notifs:

  if((notif.selection is None) or (notif.to is None)):
    logger.error("Critical fields are None in notification id: %s; label: %s" % (notif.id, notif.label))

  else:

    if(notif.internal):
      url = "http://fits/summary/%s/%s" % (options.date, notif.selection)
    else:
      url = "http://fits/summary/nolinks/%s/%s" % (options.date, notif.selection)

    logger.debug("URL is: %s" % url)
    f = urllib2.urlopen(url)
    html = f.read()
    f.close()

    match = cre.search(html)

    if(match):

      subject = "New Data for %s" % notif.selection
      logger.info(subject)

      msg = MIMEMultipart()

      text = "New data has been taken for %s. The attached html file gives details.\n\n" % (notif.selection)
      if(notif.internal):
        text += "The fits storage summary table for this data be found at: %s\n\n" % url
      else:
        text += "Access to all Gemini data is via the Gemini Science Archive at http://www1.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/gsa/\n"
        text += "Data Quality assessment and data package release will proceed as normal over the next few days."

      part1 = MIMEText(text, 'plain')
      part2 = MIMEText(html, 'html')

      msg['Subject'] = subject
      msg['From'] = options.fromaddr
      msg['To'] = notif.to
      msg['Cc'] = notif.cc
      msg['Reply-To'] = options.replyto

      msg.attach(part1)
      msg.attach(part2)

      fulllist = []
      tolist = notif.to.split(',')
      fulllist += tolist
      if(notif.cc):
        cclist = notif.cc.split(',')
        fulllist += cclist
      # For now, I Bcc myself on all the emails to see that it's working...
      fulllist.append('phirst@gemini.edu')

      try:
        logger.info("Sending Email- To: %s; CC: %s; Subject: %s" % (msg['To'], msg['Cc'], msg['Subject']))
        smtp = smtplib.SMTP(mailhost)
        smtp.sendmail(options.fromaddr, fulllist, msg.as_string())
        retval = smtp.quit()
        logger.info("SMTP seems to have worked OK: %s" % str(retval))
      except smtplib.SMTPRecipientsRefused:
        logger.error("Error sending mail message - Exception: %s: %s" % (sys.exc_info()[0], sys.exc_info()[1]))

logger.info("YouveGotDataEmail.py exiting normally")
