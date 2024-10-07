import pytest
import smtplib

from fits_storage.logger import logger, setdemon

from fits_storage.config import get_config

# In order to actually run this, you need to swap the comment and active lines
# below, and set smtp_server and email_from in your local configuration
# for test_send_email, and set email_errors_to for test_logger_email()
fsc = get_config(builtinonly=True, reload=True)
#fsc = get_config()

@pytest.mark.skipif(fsc.email_from == '',
                    reason='Current config does not provide email config')
def test_send_email():
    message = "This is a test email from the FitsStorage send_email test"
    subject = "FitsStorage test email"
    mailto = ['fitsadmin@gemini.edu']

    msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % \
          (fsc.email_from, ", ".join(mailto), subject, message)

    server = smtplib.SMTP(fsc.smtp_server)
    server.sendmail(fsc.email_from, mailto, msg)
    server.quit()

def test_logger_email():
    setdemon(True)
    logger.debug("This is a debug message and should not go in an email")
    logger.error("This is an error message and should go in an email")
    logger.critical("This is a critical message and should go in an email")