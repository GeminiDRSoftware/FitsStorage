import pytest
import smtplib

from fits_storage.config import get_config

# In order to actually run this, you need to swap the comment and active lines
# below, and populate smtp_server and email_from in your local configuration
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
