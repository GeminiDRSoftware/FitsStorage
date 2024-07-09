#!/usr/bin/env python3

import requests
from requests.adapters import HTTPAdapter, Retry
import re
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from optparse import OptionParser

from fits_storage.server.orm.notification import Notification
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope

from fits_storage.config import get_config


def get_and_fix_emails(emails):
    retval = list()
    if not emails:
        return retval
    emails = emails.split(',')
    for email in emails:
        if ' ' in email:
            check_is_multiple_emails = email.strip().split(' ')
            if False not in ['@' in e for e in check_is_multiple_emails]:
                for e in check_is_multiple_emails:
                    if e.strip() != "":
                        retval.append(e.strip())
            else:
                if email.strip() != "":
                    retval.append(email.strip())
        else:
            if email.strip() != "":
                retval.append(email.strip())
    return retval

parser = OptionParser()
parser.add_option("--date", action="store", dest="date", default="today",
                  help="Specify an alternate date to check for data from")
parser.add_option("--check", action="store_true", dest="check",
                  help="Notify CS of data set to CHECK")
parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

fsc = get_config()
if not fsc.email_from:
    logger.error("No email_from defined in Fits Storage Configuration. Exiting")
    exit(1)

warning_cre = re.compile(
    r'WARNING: I didn\'t recognize the following search terms')
fitscre = re.compile(r'\.fits')

logger.info("send_new_data_notification_emails.py starting up at %s. "
            "Processing date %s", datetime.datetime.now(), options.date)

if options.check:
    text_tmpl = """\
    New data has been marked with QA state CHECK for {sel}. 
    The attached html file gives details.

    Please check the data and set the QA state appropriately.
    """
else:
    text_tmpl = """\
    New data has been taken for {sel}. The attached html file gives details.

    The archive search for this data may be found at: {form_url}

    Data Quality assessment will proceed as normal over the next few days.
    """

# Parse out "today" in the date. This works on the website, but if they
# wait a day before clicking the link, they'll get the wrong day.
if options.date == "today":
    options.date = datetime.datetime.utcnow().strftime("%Y%m%d")

# Configure the URL base
url_base = "https://archive.gemini.edu"

# For the data set to check notifications, all should be on the local
# fits server
if options.check:
    url_base = "http://%s" % fsc.fits_server_name

# The project / email list. Get from the database
with session_scope() as session:
    for notif in session.query(Notification):
        try:
            if (notif.selection is None) or (notif.piemail is None):
                logger.error("Critical fields are None in notification id: "
                             "%s; label: %s, piemail: %s",
                             notif.id, notif.label, notif.piemail)
                continue

            selection = notif.selection
            if options.check:
                selection += '/CHECK'
            url = f"{url_base}/summary/nolinks/night={options.date}/{selection}"
            searchform_url = f"{url_base}/searchform/night={options.date}" \
                             f"/{notif.selection}"

            logger.debug("URL is: %s", url)

            try:
                s = requests.Session()
                retries = Retry(total=5, backoff_factor=1)
                s.mount('http://', HTTPAdapter(max_retries=retries))

                r = s.get(url, timeout=10)
                html = r.text
            except Exception:
                html = ''
                logger.error("Unable to fetch %s for notification id %d %s",
                             url, notif.id, notif.selection, exc_info=True)

            if warning_cre.search(html):
                logger.warn("Invalid selection seen when querying archive: "
                            "%s" % notif.selection)
                continue
            if fitscre.search(html):
                if options.check:
                    subject = "Data set to CHECK for %s" % notif.selection
                else:
                    subject = "New Data for %s" % notif.selection
                logger.info(subject)

                msg = MIMEMultipart()

                text = text_tmpl.format(sel=notif.selection,
                                        form_url=searchform_url)

                part1 = MIMEText(text, 'plain')
                part2 = MIMEText(html, 'html')

                msg['Subject'] = subject
                msg['From'] = fsc.email_from
                if options.check:
                    msg['To'] = notif.csemail
                    msg['Cc'] = ''
                else:
                    msg['To'] = notif.emailto
                    msg['Cc'] = notif.emailcc
                    msg['Reply-To'] = fsc.email_replyto

                msg.attach(part1)
                msg.attach(part2)

                fulllist = get_and_fix_emails(msg['To'])
                if msg['Cc']:
                    # Don't make this an .append, it needs to be a +=
                    fulllist += get_and_fix_emails(msg['Cc'])

                # Bcc fitsadmin on all the emails...
                fulllist.append('fitsadmin@gemini.edu')

                try:
                    logger.info("Sending Email- "
                                "To: %s; CC: %s; Subject: %s",
                                msg['To'], msg['Cc'], msg['Subject'])
                    logger.debug("Full address list: %s", fulllist)
                    smtp = smtplib.SMTP(fsc.smtp_server)
                    smtp.sendmail(fsc.email_from, fulllist,
                                  msg.as_string())
                    retval = smtp.quit()
                    logger.info("SMTP seems to have worked OK: %s",
                                str(retval))
                except smtplib.SMTPRecipientsRefused:
                    logger.error("Error sending notification email mail: "
                                 "Id %d, selection %s", notif.id,
                                 notif.selection, exc_info=True)

        except Exception:
            logger.error("Unhandled Error sending email notification: "
                         "id %d, selection %s", notif.id, notif.selection,
                         exc_info=True)

logger.info("send_new_data_notification_emails.py exiting normally at %s",
            datetime.datetime.now())
