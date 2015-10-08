import urllib2
import sys
import re
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from optparse import OptionParser

from fits_storage.orm import session_scope
from fits_storage.orm.notification import Notification
from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.fits_storage_config import fits_servername, use_as_archive, smtp_server

parser = OptionParser()
parser.add_option("--emailfrom", action="store", dest="fromaddr", default="fitsdata@gemini.edu", help="Email Address to send from")
parser.add_option("--replyto", action="store", dest="replyto", default="fitsadmin@gemini.edu", help="Set a Reply-To email header")
parser.add_option("--date", action="store", dest="date", default="today", help="Specify an alternate date to check for data from")
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()
# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# ISG have local smtp servers set up on the fits hosts (mkofits1 and cpofits1) that relay mail to the gemini smtp
# servers without needing to authenticate. Otherwise, the gemini smtp will not relay to external addresses.

cre = re.compile(r'\.fits')

logger.info("YouveGotDataEmail.py starting for date %s" % options.date)

text_tmpl = """\
New data has been taken for {sel}. The attached html file gives details.

The archive search for this data may be found at: {form_url}

Data Quality assessment will proceed as normal over the next few days.
"""

# Parse out "today" in the date. This works on the web site, but if they wait a day before
# clicking the link, they'll get the wrong day.
if options.date == "today":
    options.date = datetime.datetime.utcnow().strftime("%Y%m%d")

# Configure the URL base
if use_as_archive:
    url_base = "https://archive.gemini.edu"
else:
    url_base = "http://%s" % fits_servername

# The project / email list. Get from the database
with session_scope() as session:
    for notif in session.query(Notification):
        if (notif.selection is None) or (notif.to is None):
            logger.error("Critical fields are None in notification id: %s; label: %s", notif.id, notif.label)
        else:
            url = "%s/summary/nolinks/%s/%s" % (url_base, options.date, notif.selection)
            searchform_url = "%s/searchform/%s/%s" % (url_base, options.date, notif.selection)

            logger.debug("URL is: %s", url)
            html = urllib2.urlopen(url).read()

            if cre.search(html):
                subject = "New Data for %s" % notif.selection
                logger.info(subject)

                msg = MIMEMultipart()

                text = text_tmpl.format(sel=notif.selection, form_url=searchform_url)

                part1 = MIMEText(text, 'plain')
                part2 = MIMEText(html, 'html')

                msg['Subject'] = subject
                msg['From'] = options.fromaddr
                msg['To'] = notif.to
                msg['Cc'] = notif.cc
                msg['Reply-To'] = options.replyto

                msg.attach(part1)
                msg.attach(part2)

                fulllist = notif.to.split(',')
                if notif.cc:
                    fulllist += notif.cc.split(',')
                # For now, Bcc fitsadmin on all the emails to see that it's working...
                fulllist.append('fitsadmin@gemini.edu')

                try:
                    logger.info("Sending Email- To: %s; CC: %s; Subject: %s", msg['To'], msg['Cc'], msg['Subject'])
                    smtp = smtplib.SMTP(smtp_server)
                    smtp.sendmail(options.fromaddr, fulllist, msg.as_string())
                    retval = smtp.quit()
                    logger.info("SMTP seems to have worked OK: %s", str(retval))
                except smtplib.SMTPRecipientsRefused:
                    logger.error("Error sending mail message - Exception: %s: %s" % (sys.exc_info()[0], sys.exc_info()[1]))

logger.info("YouveGotDataEmail.py exiting normally")
