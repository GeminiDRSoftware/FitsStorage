"""
This module handles the web 'user' functions - creating user accounts, login / logout, password reset etc
"""

from orm import sessionfactory

from orm.user import User

from fits_storage_config import fits_servername, smtp_server

# This will only work with apache
from mod_python import apache
from mod_python import Cookie
from mod_python import util

import re
import datetime
import time
import smtplib
from email.mime.text import MIMEText

def request_account(req, things):
    """
    Generates and handles web form for requesting new user accounts
    """
    # Process the form data first if there is any
    formdata = util.FieldStorage(req)
    request_attempted = False
    valid_request = None
    reason_bad = None

    username = ''
    fullname = ''
    email = ''

    # Contruct the thing_string for the url to link back to their form
    if(things):
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if(len(formdata.keys()) > 0):
        request_attempted = True
        if('username' in formdata.keys()):
            username = formdata['username'].value
        if('fullname' in formdata.keys()):
            fullname = formdata['fullname'].value
        if('email' in formdata.keys()):
            email = formdata['email'].value

        # Validate
        valid_request = False
        if username == '':
            reason_bad = "No Username supplied"
        elif(not username.isalnum()):
            reason_bad = "Username may only contain alphanumeric characters"
        elif(len(username) < 5):
            reason_bad = "Username too short. Must be at least 5 characters"
        elif(username_inuse(username)):
            reason_bad = 'Username is already in use, choose a different one. If this is your username, you can <a href="/password_reset">reset your password</a>.'
        elif(fullname == ''):
            reason_bad = "No Full name supplied"
        elif(len(fullname) < 5):
            reason_bad = "Full name must be at least 5 characters"
        elif(email == ''):
            reason_bad = "No Email address supplied"
        elif(('@' not in email) or ('.' not in email)):
            reason_bad = "Not a valid Email address"
        elif(',' in email):
            reason_bad = "Email address cannot contain commas"
        else:
            valid_request = True

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive new account request</title></head><body>")
    # Uncomment next line for form debugging
    # req.write("<P>formdata: %s</P>" % formdata)
    req.write("<h1>New Account Request</h1>")

    if(valid_request):
        req.write("<h2>Processing your request...</h2>")
        req.write('<TABLE>')
        req.write('<TR><TD>Username:</TD><TD>%s</TD></TR>' % username)
        req.write('<TR><TD>Full Name:</TD><TD>%s</TD></TR>' % fullname)
        req.write('<TR><TD>Email:</TD><TD>%s</TD></TR>' % email)
        req.write('</TABLE>')
        try:
            session = sessionfactory()
            newuser = User(username)
            newuser.fullname = fullname
            newuser.email = email
            session.add(newuser)
            session.commit()
            emailed = send_password_reset_email(newuser.id)
        except:
            req.write("<P>ERROR: Adding new user failed. Sorry. Please contact helpdesk</P>")
            req.write('</body></html>')
            return apache.OK
        finally:
            session.close()
        req.write('<P>Account request processed.</P>')
        if(emailed):
            req.write('<P>You should shortly receive an email with a link to set your password and activate your account.</P>')
            req.write("<P>If you don't get the email, please contact the Gemini helpdesk.</P>")
            if(thing_string):
                req.write('<P>After you set your password and log in using another browser tab, you can ')
                req.write('<a href="/searchform%s">click here to return to your search.</a></p>' % thing_string)
        else:
            req.write('<P>Sending you a password reset email FAILED. Please contact Gemini Helpdesk. Sorry.</P>')
        req.write('</body></html>')
        return apache.OK

    else:
        # New account request was not valid
        if(request_attempted):
            req.write("<P>Your request was invalid. %s. Please try again.</P>" % reason_bad)

        # Send the new account form
        req.write('<FORM action="/request_account%s" method="POST">' % thing_string)
        req.write('<P>Fill out and submit this short form to request a Gemini Archive account. You must provide a valid email address - we will be emailing you a link to activate your account and set a password.</P>')
        req.write('<TABLE>')

        # username row
        req.write('<TR><TD><LABEL for="username">Username</LABEL><TD>')
        req.write('<TD><INPUT type="text" size=16 name="username" value=%s></INPUT></TD></TR>' % username)

        # fullname row
        req.write('<TR><TD><LABEL for="fullname">Full Name</LABEL><TD>')
        req.write('<TD><INPUT type="text" size=32 name="fullname" value=%s></INPUT></TD></TR>' % fullname)

        # email address row
        req.write('<TR><TD><LABEL for="email">Email Address</LABEL><TD>')
        req.write('<TD><INPUT type="text" size=32 name="email" value=%s></INPUT></TD></TR>' % email)

        # Some kind of captcha here.

        req.write('</TABLE>')
        req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
        req.write('</FORM>')
        req.write("</body></html>")
        return apache.OK

def send_password_reset_email(userid):
    """
    Sends the user a password reset email
    """

    message_text = """
  A password reset has been requested for the Gemini Archive account
registered to this email address. If you did not request a password
reset, you can safely ignore this email, though if you get several 
spurious reset request emails, please file a helpdesk ticket to let 
us know. Assuming that you requested this password reset, please click
on the link below or paste it into your browser to reset your password.
The reset link is only valid for 15 minutes, so please do that promptly.

    """

    session = sessionfactory()
    try:
        query = session.query(User).filter(User.id == userid)
        user = query.one()
        token = user.generate_reset_token()
        session.commit()
        username = user.username
        email = user.email
        fullname = user.fullname
    except:
        pass
    finally:
        session.close()

    url = "http://%s/password_reset/%d/%s" % (fits_servername, userid, token)

    message = "Hello %s,\n" % fullname
    message += message_text
    message += 'The username for this account is: %s\n' % username
    message += "%s\n\n" % url
    message += "Regards,\n    Gemini Observatory Archive\n\n"

    fromaddr = 'fitsadmin@gemini.edu'
    tolist = [email, fromaddr]
    msg = MIMEText(message)
    msg['Subject'] = 'Gemini Archive Password Reset link'
    msg['From'] = fromaddr
    msg['To'] = email

    try:
        smtp = smtplib.SMTP(smtp_server)
        smtp.sendmail(fromaddr, tolist, msg.as_string())
    except:
        return False

    return True

def password_reset(req, things):
    """
    Handles users clicking on a password reset link that we emailed them.
    Check the reset token for validity, if valid the present them with a
    password reset form and process it when submitted.
    """

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive password reset request</title></head><body>")

    # debug print
    # req.write('<P>things: %s</P>' % things)

    if(len(things) != 2):
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.OK

    # Extract and validate the things from the URL
    userid = things[0]
    token = things[1]

    try:
        userid = int(userid)
    except:
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.OK

    if(len(token) != 56):
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.OK

    # OK, seems possibly legit. Check with database
    try:
        session = sessionfactory()
        query = session.query(User).filter(User.id == userid)
        user = query.first()
        if(user is None):
            req.write('<P>Invalid request.</P>')
            req.write('</body></html>')
            return apache.OK
        elif(user.reset_token_expires < datetime.datetime.now()):
            req.write('<P>This reset link has expired. They are only valid for 15 minutes. Sorry. Please request a new one and try again.</P>')
            req.write('</body></html>')
            return apache.OK
        elif(user.reset_token != token):
            req.write("<P>This is not a valid password reset link. Sorry. If you pasted the link, please check it didn't get truncated and try again, or request a new reset.</P>")
            req.write('</body></html>')
            return apache.OK
        else:
            # Appears to be valid
            req.write("<H1>Gemini Observatory Archive Password Reset</H1>")
    except:
        # pass
        raise
    finally:
        session.close()

    # If we got this far we have a valid request.
    # Did we get a submitted form?
    request_attempted = False
    request_valid = False
    reason_bad = ''
    formdata = util.FieldStorage(req)
    password = None
    again = None
    if(len(formdata.keys()) > 0):
        request_attempted = True
        if('password' in formdata.keys()):
            password = formdata['password'].value
        if('again' in formdata.keys()):
            again = formdata['again'].value

        # Validate
        if(password is None):
            reason_bad = 'No new Password supplied'
        elif(password != again):
            reason_bad = 'Password and Password again do not match'
        elif(bad_password(password)):
            reason_bad = 'Bad password - must be at least 8 characters and contain letters and numbers'
        else:
            request_valid = True

    if(request_valid):
        req.write('<H2>Processing your request...</H2>')
        try:
            session = sessionfactory()
            query = session.query(User).filter(User.id == userid)
            user = query.one()
            if(user.validate_reset_token(token)):
                user.reset_password(password)
                session.commit()
                req.write('<P>Password has been reset.</P>')
                req.write('<p><a href="/login">Click here to log in.</a></p>')
                return apache.OK
            else:
                req.write("<P>Link is no longer valid. Please request a new one.</P>")
                req.write('</body></html>')
                return apache.OK
        except:
            # pass
            raise
        finally:
            session.close()

    if(request_attempted):
        req.write("<P>Your request was invalid. %s. Please try again.</P>" % reason_bad)

    # Send the new account form
    req.write('<FORM action="/password_reset/%d/%s" method="POST">' % (userid, token))
    req.write('<P>Fill out and submit this form to reset your password. Password must be 8 characters or more and must contain at least some letters and numbers.</P>')
    req.write('<TABLE>')
    req.write('<TR><TD><LABEL for="password">New Password</LABEL></TD>')
    req.write('<TD><INPUT type="password" size=16 name="password"</TD></TR>')
    req.write('<TR><TD><LABEL for="again">New Password Again</LABEL></TD>')
    req.write('<TD><INPUT type="password" size=16 name="again"</TD></TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.OK

def change_password(req, things):
    """
    Handles a logged in user wanting to change their password.
    """
    # Present and process a change password form. User must be logged in,
    # and know their current password.

    # Process the form data first if there is any
    formdata = util.FieldStorage(req)
    request_attempted = False
    valid_request = None
    reason_bad = None
    sucessfull = False

    oldpassword = ''
    newpassword = ''
    newagain = ''

    # Construct the things_string to link back to the current form
    if(things):
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if(len(formdata.keys()) > 0):
        request_attempted = True
        if('oldpassword' in formdata.keys()):
            oldpassword = formdata['oldpassword'].value
        if('newpassword' in formdata.keys()):
            newpassword = formdata['newpassword'].value
        if('newagain' in formdata.keys()):
            newagain = formdata['newagain'].value

        # Validate what came in
        valid_request = False
        
        if(oldpassword == ''):
            reason_bad = 'No old password supplied'
        elif(newpassword == ''):
            reason_bad = 'No new password supplied'
        elif(newagain == ''):
            reason_bad = 'No new password again supplied'
        elif(bad_password(newpassword)):
            reason_bad = 'Bad password - must be at least 8 characters and contain letters and numbers'
        elif(newpassword != newagain):
            reason_bad = 'New Password and New Password Again do not match'
        else:
            valid_request = True

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Password reset request</title></head><body>")

    if(valid_request):
        req.write('<H2>Processing your request...</H2>')
        try:
            session = sessionfactory()
            user = userfromcookie(session, req)
            if(user is None):
                valid_request = False
                reason_bad = 'You are not currently logged in'
            elif(user.validate_password(oldpassword) is False):
                valid_request = False
                reason_bad = 'Current password not correct'
            else:
                user.change_password(newpassword)
                session.commit()
                req.write('<p>Password has been changed</p>')
                if(things):
                    req.write('<p><a href="/searchform%s">Click here to go back to your searchform</p>' % thing_string)
                else:
                    req.write('<p><a href="/searchform">Click here to go to the searchform</p>')
                sucessfull = True
        finally:
            session.close()

    if(request_attempted is True and valid_request is False):
        req.write('<h2>Request not valid:</h2>')
        req.write('<p>%s</p>' % reason_bad)

    if(not sucessfull):
        # Send the password change form
        req.write('<FORM action="/change_password%s" method="POST">' % thing_string)
        req.write('<P>Fill out and submit this form to change your password. Password must be 8 characters or more and must contain at least some letters and numbers.</P>')
        req.write('<TABLE>')
        req.write('<TR><TD><LABEL for="oldpassword">Current Password</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="oldpassword"</TD></TR>')
        req.write('<TR><TD><LABEL for="newpassword">New Password</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="newpassword"</TD></TR>')
        req.write('<TR><TD><LABEL for="newagain">New Password Again</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="newagain"</TD></TR>')
        req.write('</TABLE>')
        req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
        req.write('</FORM>')


    req.write("</body></html>")
    return apache.OK

def request_password_reset(req):
    """
    Generate and process a web form to request a password reset
    """
    # Process the form data first if thre is any
    formdata = util.FieldStorage(req)
    request_valid = None

    username = None
    email = None

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Password reset request</title></head><body>")

    # Parse the form data here
    if(len(formdata.keys()) > 0):
        if('thing' in formdata.keys()):
            thing = formdata['thing'].value

        # Validate
        if(username_inuse(thing)):
            # They gave us a valid username
            username = thing     
            request_valid = True
        elif(email_inuse(thing)):
            # They gave us a valid email address
            email = thing
            request_valid = True
        else:
            request_valid = False
            req.write('<P>That is not a valid username or email address in our system. Maybe you need to create a new account</P>')

    if(request_valid):
        # Try to process it
        req.write('<P>Processing request...</P>')
        try:
            session = sessionfactory()
            query = session.query(User)
            if(username):
                query = query.filter(User.username == username)
            elif(email):
                query = query.filter(User.email == email)
            else:
                # Something is wrong
                req.write('<P>Error: no valid username or email. This should not happen.</P>')
                return apache.OK
            users = query.all()
            if(len(users) > 1):
                req.write("<P>Multiple usernames are using this email address. We'll send an email for each username. Please contact the Gemini Helpdesk to sort this out.</P>")
            for user in users:
                req.write("<P>Sending password reset email for username: %s</P>" % user.username)
                emailed = send_password_reset_email(user.id)
                if(emailed):
                    req.write('<P>You should shortly receive an email with a link to set your password and activate your account.</P>')
                    req.write("<P>If you don't get the email, please contact the Gemini helpdesk.</P>")
                else:
                    req.write('<P>Sending you a password reset email FAILED. Please contact Gemini Helpdesk. Sorry.</P>')
        except:
            #pass
            raise
        finally:
            session.close()

        req.write('</body></html>')
        return apache.OK

    # Send the reset form
    req.write('<FORM action="/request_password_reset" method="POST">')
    req.write("<P>Enter your Gemini Archive Username or the Email address you registered with us when you created the account in the box below and bit submit. We'll send you an email containing a link to follow to reset your password</P>")
    req.write('<INPUT type="text" size=32 name="thing"</INPUT>')

    # Some kind of captcha here.

    req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.OK

def login(req, things):
    """
    Presents and processes a login form
    Sends session cookie if sucessfull
    """
    # Process the form data first if there is any
    formdata = util.FieldStorage(req)
    request_attempted = False
    valid_request = None
    reason_bad = None

    username = ''
    password = ''
    cookie = None

    # Rebuild the thing_string for the url
    if(things):
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if(len(formdata.keys()) > 0):
        request_attempted = True
        if('username' in formdata.keys()):
            username = formdata['username'].value
        if('password' in formdata.keys()):
            password = formdata['password'].value

        # Validate
        valid_request = False
        if username == '':
            reason_bad = "No Username supplied"
        elif password == '':
            reason_bad = "No Password supplied"
        elif (not username_inuse(username)):
            reason_bad = "Username / password not valid"
        else:
            # Find the user and check if the password is valid
            try:
                session = sessionfactory()
                query = session.query(User).filter(User.username == username)
                user = query.one()
                if(user.validate_password(password)):
                    # Sucessfull login
                    cookie = user.log_in()
                    valid_request = True
                    session.commit()
                else:
                    reason_bad = "Username / password not valid"
            finally:
                session.close()

    req.content_type = "text/html"
    if(valid_request):
        # Cookie expires in 1 year
        cookie_obj = Cookie.Cookie('gemini_archive_session', cookie, expires = time.time()+31536000, path = "/")
        Cookie.add_cookie(req, cookie_obj)

    req.write("<html><head><title>Gemini Archive log in</title></head><body>")

    if(valid_request):
        req.write('<P>Welcome, you are sucessfully logged in to the Gemini Archive.</P>')
        if(things):
            url = "/searchform"
            for thing in things:
                url += "/%s" % thing
            req.write('<p><a href="%s">Click here to go back to your search form</a></p>' % url)
            req.write('<p><a href="/searchform">Click here to go to an empty search form</a></p>')
        else:
            req.write('<p><a href="/searchform">Click here to go to the search form</a></p>')

        req.write('</body></html>')
        return apache.OK

    if(request_attempted):
        req.write('<P>Log-in did not suceed: %s. Please try again.</P>' % reason_bad)

    req.write('<FORM action="/login%s" method="POST">' % thing_string)
    req.write('<TABLE>')
    req.write('<TR><TD><LABEL for="username">Username</LABEL><TD>')
    req.write('<TD><INPUT type="text" size=16 name="username" value=%s></INPUT></TD></TR>' % username)
    req.write('<TR><TD><LABEL for="password">Password</LABEL><TD>')
    req.write('<TD><INPUT type="password" size=16 name="password"></INPUT></TD></TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.OK


def logout(req):
    """
    Log out all archive sessions for this user
    """
    # Do we have a session cookie?
    cookie = None
    cookies = Cookie.get_cookies(req)
    if(cookies.has_key('gemini_archive_session')):
        cookie = cookies['gemini_archive_session'].value

    if(cookie):
        # Find the user that we are
        try:
            session = sessionfactory()
            query = session.query(User).filter(User.cookie == cookie)
            users = query.all()
            if(len(users) == 0):
                # We weren't really logged in. 
                pass
            elif(len(users) > 1):
                # Eeek, multiple users with the same session cookie!?!?!
                req.log_error("Logout - Multiple Users with same session cookie: %s" % cookie)
            for user in users:
                user.log_out_all() 
                session.commit()
        finally:
            session.close()

        Cookie.add_cookie(req, 'gemini_archive_session', '', expires = time.time())
    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive log out</title></head><body>")
    req.write('<P>You are sucessfully logged out of the Gemini Archive.</P>')
    req.write('<p>You might now want to go to the <a href="/">archive home page</a></p>')
    req.write('</body></html>')
    return apache.OK

def whoami(req, things):
    """
    Tells you who you are logged in as, and presents the account maintainace links
    """
    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Who Am I</title>")
    #req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/whoami.css">')
    req.write("</head><body>")

    username = None
    # Find out who we are if logged in
    try:
        session = sessionfactory()
        user = userfromcookie(session, req)
    finally:
        session.close()

    if(user is not None):
        username = user.username
        fullname = user.fullname

    # Construct the "things" part of the URL for the link that want to be able to 
    # take you back to the same form contents
    if(len(things)):
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    req.write('<span id="whoami">')
    if(username):
        req.write('&#x1f464; %s &#9662' % username)
        req.write('<ul class="whoami">')
        req.write('<li class="whoami">%s</li>' % fullname)
        req.write('<li class="whoami"><a href="/logout">Log Out</a></li>')
        req.write('<li class="whoami"><a href="/change_password%s">Change Password</a></li>' % thing_string)
        req.write('<li class="whoami"><a href="/my_programs%s">My Programs</a></li>' % thing_string)
    else:
        req.write('Not logged in')
        req.write('<ul class="whoami">')
        req.write('<li class="whoami"><a href="/request_account%s">Request Account</a></li>' % thing_string)
        req.write('<li class="whoami"><a href="/login%s">Login</a></li>' % thing_string)
        req.write('</ul>')

    req.write('</span>')
    req.write('</body></html>')
    return apache.OK



def email_inuse(email):
    """
    Check the database to see if this email is already in use. Returns True if it is, False otherwise
    """
    try:
	session = sessionfactory()
	query = session.query(User).filter(User.email == email)
	num = query.count()

        if(num == 0):
            return False
        else:
            return True
    finally:
        session.close()

def username_inuse(username):
    """
    Check the database to see if a username is already in use. Returns True if it is, False otherwise
    """
    try:
        session = sessionfactory()
        query = session.query(User).filter(User.username == username)
        num = query.count()

        if(num == 0):
            return False
        else:
            return True
    finally:
        session.close()

digits_cre = re.compile('\d')
lower_cre = re.compile('[a-z]')
upper_cre = re.compile('[A-Z]')
def bad_password(candidate):
    """
    Checks candidate for compliance with password rules.
    Returns True if it is bad, False if it is good
    """
    if(len(candidate) < 8):
        return True
    elif(not bool(digits_cre.search(candidate))):
        return True
    elif(not bool(lower_cre.search(candidate))):
        return True
    elif(not bool(upper_cre.search(candidate))):
        return True
    else:
        return False

def userfromcookie(session, req):
    """
    Given a database session and request object, get the session cookie
    from the request object and find and return the user object, 
    or None if it is not a valid session cookie
    """

    # Do we have a session cookie?
    cookie = None
    cookies = Cookie.get_cookies(req)
    if(cookies.has_key('gemini_archive_session')):
        cookie = cookies['gemini_archive_session'].value
    else:
        # No session cookie, not logged in
        return None

    # Find the user that we are
    query = session.query(User).filter(User.cookie == cookie)
    user = query.first()
    if(user is None):
        # This is not a valid session cookie
        return None
    else:
        return user

