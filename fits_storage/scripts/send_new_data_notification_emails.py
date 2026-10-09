#!/usr/bin/env python3

import requests
from requests.adapters import HTTPAdapter, Retry
import re
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from argparse import ArgumentParser

from fits_storage.server.orm.notification import Notification
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.db import session_scope
from fits_storage import utcnow

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


parser = ArgumentParser()
parser.add_argument("--date", action="store", dest="date", default="today",
                  help="Specify an alternate date to check for data from")
parser.add_argument("--check", action="store_true", dest="check",
                  help="Notify CS of data set to CHECK")
parser.add_argument("--reduced", action="store_true", dest="reduced",
                    help="Notify about Reduced data, not Raw data")
parser.add_argument("--reduced-preimage", action="store_true", dest="reduced_preimage",
                    help="Send Specific notification about Reduced pre-imaging data")
parser.add_argument("--notification-id", action="store", type=int,
                    dest="notification_id",
                    help="Only use this specific notification id. Useful for testing only")
parser.add_argument("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")
parser.add_argument("--dryrun", action="store_true", dest="dryrun",
                  help="Do not actually send emails")
options = parser.parse_args()

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
elif options.reduced_preimage:
    text_tmlp = """\
    Gemini MOS preimaging data were recently taken for your program and automatically reduced pre-imaging data products are available in the archive for {sel}. The attached html file gives details.
        
    This e-mail contains instructions on how to proceed. MOS masks are
    usually cut once per week. Queue program PIs are suggested to submit
    their masks at least one week before the desired cutting date to
    allow sufficient time for NGO masks checks.

    Classical program PIs must submit at least 3 weeks prior to their 
    scheduled run.

    Because your program involves MOS observations, the next step for you is
    to design the mask(s) for these observations. In order to accomplish
    this on a short time scale, the reduced co-added image(s) of your
    target(s) have beenvmade available to you via the Gemini Observatory Archive at 

    {form_url}
    
    Instructions on how to proceed and the software you will need have been
    put on the Gemini web site. Please see:

    http://www.gemini.edu/sciops/instruments/gmos/multi-object-spectroscopy

    Please make sure you are using the latest version of the mask making
    software. 

    Using the information on these web pages you will be able to construct
    an Object Definition Table (this is a FITS table) that contains your
    mask design. We need this table back from you. The Object Definition
    Table should be submitted using the Observing Tool by using the File
    attachment tab. For instructions on how to upload a file attachment using the OT, see:

    https://www.gemini.edu/observing/phase-ii/ot/ot-description/other#Tabs

    Please make a note in the file attachment description stating the name of the
    image that is associated with the submitted mask file(s).

    Submitting your masks as soon as possible greatly increases the chance
    of getting your observations done. Gemini Observatory reserves the right
    to not cut masks that have not been submitted sooner than 6 weeks before
    the end of the semester, if the queue coordinators deem the MOS
    observations likely to not be able to be scheduled. Exceptions will be
    made for programs with roll-over status if the target is observable for
    more than 6 weeks from the time the mask was submitted. 

    Your phase II MOS observations should already be defined. If this is
    not yet done, or if there are any changes that you wish to make at this
    time, please contact your Principal Support Scientist right away. Make sure
    you have the latest version of the Observing Tool installed, see:

    http://www.gemini.edu/sciops/OThelp/otIndex.html

    For general Phase II instructions and a Phase II checklist for GMOS, please see

    http://www.gemini.edu/sciops/instruments/GMOS/observation-preparation

    When you add your MOS observations make sure you use the same
    coordinates, guide stars, and position angles
    as used for your pre-imaging observations. Once completed, set the
    observation status to 'For Review' and synchronize it to the database.
    For any general questions about the phase II or
    the mask design process, please contact the Gemini Helpdesk:

    http://www.gemini.edu/sciops/helpdesk/

    Best regards,    
        Gemini Observatory
        
    """
elif options.reduced:
    text_tmpl = """\
    Automatically reduced data products are available in the archive for {sel}. The attached html file gives details.
    
    The archive search for this data may be found at: {form_url}
    
    Processed (ie reduced) data files in the archive generally have a processing classification specified as "Science-Quality" or"Quick-Look". 

    Science-Quality data have been reduced by data reduction software which is not known to contain any bugs or deficiencies which would significantly affect the quality of the reduced data products.
    The best available calibrations have been applied, and the calibrations are also considered Science-Quality.

    Science Quality data are intended to be suitable for science use, however in most cases they have been generated automatically and have not been reviewed by an expert. 
    There will likely be cases where the automatic reduction does not provide good results and the onus is on the end user to verify that the data and reduction meets their requirements. They are reduced in a general manner which we believe is applicable to most science cases, however some science cases will require that the data be re-reduced with reduction techniques specific to the particular scientific use case.

    Quick-Look data do not meet one or more of the above criteria for designating the data as Science-Quality.

    Quick-Look reduced data are intended to be used to assess simply whether the data are of interest to the user, in which case re-reduction or manual processing may be appropriate.
    """
else:
    text_tmpl = """\
    New data has been taken for {sel}. The attached html file gives details.

    The archive search for this data may be found at: {form_url}
    
    Automatic reduction will have been initiated for modes where it is available, a separate notification will be send for any resulting reduced data products.
    
    Data Quality assessment will proceed as normal over the next few days.
    """

# Parse out "today" in the date. This works on the website, but if they
# wait a day before clicking the link, they'll get the wrong day.
if options.date == "today":
    options.date = utcnow().strftime("%Y%m%d")

# Configure the URL base
url_base = "https://archive.gemini.edu"

# For the data set to check notifications, all should be on the local
# fits server
if options.check:
    url_base = "http://%s" % fsc.fits_server_name

# The project / email list. Get from the database
with session_scope() as session:
    query = session.query(Notification)
    if options.notification_id:
        query = query.filter(Notification.id == options.notification_id)
    for notif in query:
        try:
            if (notif.selection is None) or (notif.piemail is None):
                logger.error("Critical fields are None in notification id: "
                             "%s; label: %s, piemail: %s",
                             notif.id, notif.label, notif.piemail)
                continue

            selection = notif.selection
            if options.reduced or options.reduced_preimage:
                selection += '/Science-Quality'
            else:
                selection += '/Raw'
            if options.reduced_preimage:
                selection += '/preimage'
            if options.check:
                selection += '/CHECK'
            url = f"{url_base}/summary/nolinks/night={options.date}/{selection}"
            searchform_url = f"{url_base}/searchform/night={options.date}" \
                             f"/{selection}"

            logger.debug("URL is: %s", url)

            try:
                s = requests.Session()
                s.headers.update({'User-Agent': fsc.http_user_agent})
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
                elif options.reduced_preimage:
                    subject = "Reduced Pre-image data available for %s" % notif.selection
                elif options.reduced:
                    subject = "Automatically reduced science data available for %s" % notif.selection
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

                if options.dryrun:
                    logger.info("Dryrun - NOT Sending Email- "
                                "To: %s; CC: %s; Subject: %s",
                                msg['To'], msg['Cc'], msg['Subject'])
                    logger.debug("Full address list: %s", fulllist)
                    logger.debug(part1)
                    logger.debug(part2)
                else:
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
