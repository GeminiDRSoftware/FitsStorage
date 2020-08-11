import smtplib

from fits_storage.fits_storage_config import smtp_server


def sendmail(subject, mailfrom, mailto, msglines):
    """
    Helper method for sending email.

    This wraps the boilerplate we need in various places to send an email.  This method
    handles looking up the SMTP server from our configuration, connecting, sending the
    list of strings as a message, and closing the SMTP connection.

    Parameters
    ----------

    subject : str
        Subject of the email
    mailfrom : str
        Email address for the sender
    mailto : str
        Email address for the recipient(s)
    msglines : array of str
        List of text messages to combine into the message (for instance, a list of error messages)
    """

    # make sure we have a list of strings
    if isinstance(msglines, str):
        msglines = [msglines,]
    message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (
              mailfrom, ", ".join(mailto), subject, '\n'.join(msglines))

    server = smtplib.SMTP(smtp_server)
    server.sendmail(mailfrom, mailto, message)
    server.quit()
