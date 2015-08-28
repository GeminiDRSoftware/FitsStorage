"""
This module handles the web 'user' functions - creating user accounts, login / logout, password reset etc
"""

from sqlalchemy import desc

from ..orm import sessionfactory, session_scope
from ..orm.user import User

from ..fits_storage_config import fits_servername, smtp_server
from sqlalchemy.orm.exc import NoResultFound

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
    if things:
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if len(formdata.keys()) > 0:
        request_attempted = True
        if 'username' in formdata.keys():
            username = formdata['username'].value
        if 'fullname' in formdata.keys():
            fullname = formdata['fullname'].value
        if 'email' in formdata.keys():
            email = formdata['email'].value

        # Validate
        valid_request = False
        if username == '':
            reason_bad = "No Username supplied"
        elif not username.isalnum():
            reason_bad = "Username may only contain alphanumeric characters"
        elif len(username) < 2:
            reason_bad = "Username too short. Must be at least 2 characters"
        elif username_inuse(username):
            reason_bad = 'Username is already in use, choose a different one. If this is your username, you can <a href="/login">log in</a> if you know your password, or <a href="/request_password_reset">reset your password</a> if you have forgotten it.'
        elif fullname == '':
            reason_bad = "No Full name supplied"
        elif len(fullname) < 5:
            reason_bad = "Full name must be at least 5 characters"
        elif email == '':
            reason_bad = "No Email address supplied"
        elif ('@' not in email) or ('.' not in email):
            reason_bad = "Not a valid Email address"
        elif ',' in email:
            reason_bad = "Email address cannot contain commas"
        else:
            valid_request = True

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive new account request</title></head><body>")
    # Uncomment next line for form debugging
    # req.write("<P>formdata: %s</P>" % formdata)
    req.write("<h1>New Account Request</h1>")
    req.write("<p>Please note that user accounts are for individual use and should not be shared.")
    req.write("You must not share your password with anyone and should take reasonable measures to keep it confidential.</p>")

    if valid_request:
        req.write('<TABLE>')
        req.write('<TR><TD>Username:</TD><TD>%s</TD></TR>' % username)
        req.write('<TR><TD>Full Name:</TD><TD>%s</TD></TR>' % fullname)
        req.write('<TR><TD>Email:</TD><TD>%s</TD></TR>' % email)
        req.write('</TABLE>')
        req.write("<h2>Processing your request...</h2>")
        if email.endswith("@gemini.edu"):
            req.write("<P>That looks like a Gemini Staff email address. If you would like Gemini Staff Access privileges adding to your new archive account, please contact the archive scientist to request that.</P>")
        try:
            session = sessionfactory()
            newuser = User(username)
            newuser.fullname = fullname
            newuser.email = email
            session.add(newuser)
            session.commit()
            emailed = send_password_reset_email(newuser.id)
        except:
            req.write("<P>ERROR: Adding new user failed. Sorry. Please contact helpdesk. TODO - add link to helpdesk.</P>")
            req.write('</body></html>')
            return apache.HTTP_OK
        finally:
            session.close()
        req.write('<P>Account request processed.</P>')
        if emailed:
            req.write('<P>You should shortly receive an email with a link to set your password and activate your account.</P>')
            req.write("<P>If you don't get the email, please contact the Gemini helpdesk. TODO - add link to helpdesk</P>")
            req.write('<P><a href="/searchform%s">Click here to return to your search.</a> ' % thing_string)
            req.write('After you set your password and log in using another browser tab, you can just reload or hit the submit button again and it will recognize your login</P>')
        else:
            req.write('<P>Sending you a password reset email FAILED. Please contact Gemini Helpdesk. Sorry.</P>')
        req.write('</body></html>')
        return apache.HTTP_OK

    else:
        # New account request was not valid
        if request_attempted:
            req.write("<P>Your request was invalid. %s. Please try again.</P>" % reason_bad)

        # Send the new account form
        req.write('<FORM action="/request_account%s" method="POST">' % thing_string)
        req.write('<P>Fill out and submit this short form to request a Gemini Archive account. You must provide a valid email address - we will be emailing you a link to activate your account and set a password. The email should arrive promptly, please note the activation link expires 15 minutes after it was sent. Usernames must be purely alphanumeric characters and must be at least two characters long.</P>')
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
        req.write('<INPUT type="submit" value="Submit"></INPUT>')
        req.write('</FORM>')
        req.write("</body></html>")
        return apache.HTTP_OK

def send_password_reset_email(userid):
    """
    Sends the user a password reset email
    """

    message_text = """
  A password reset has been requested for the Gemini Archive account
registered to this email address. If you did not request a password
reset, you can safely ignore this email, though if you get several 
spurious reset request emails, please file a helpdesk ticket 
(TODO - add link to helpdesk) to let us know. Assuming that you 
requested this password reset, please click on the link below or paste 
it into your browser to reset your password. The reset link is only 
valid for 15 minutes, so please do that promptly.

    """

    with session_scope() as session:
        user = session.query(User).get(userid)
        username = user.username
        email = user.email
        fullname = user.fullname
        token = user.generate_reset_token()

    url = "https://%s/password_reset/%d/%s" % (fits_servername, userid, token)

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

    if len(things) != 2:
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.HTTP_OK

    # Extract and validate the things from the URL
    userid = things[0]
    token = things[1]

    try:
        userid = int(userid)
    except:
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.HTTP_OK

    if len(token) != 56:
        req.write('<P>Invalid request.</P>')
        req.write('</body></html>')
        return apache.HTTP_OK

    # OK, seems possibly legit. Check with database
    with session_scope() as session:
        user = session.query(User).get(userid)
        if user is None:
            req.write('<P>Invalid request.</P>')
            req.write('</body></html>')
            return apache.HTTP_OK
        elif user.reset_token_expires < datetime.datetime.utcnow():
            req.write('<P>This reset link has expired. They are only valid for 15 minutes. Sorry. Please request a new one and try again.</P>')
            req.write('</body></html>')
            return apache.HTTP_OK
        elif user.reset_token != token:
            req.write("<P>This is not a valid password reset link. Sorry. If you pasted the link, please check it didn't get truncated and try again, or request a new reset.</P>")
            req.write('</body></html>')
            return apache.HTTP_OK
        else:
            # Appears to be valid
            req.write("<H1>Gemini Observatory Archive Password Reset</H1>")

    # If we got this far we have a valid request.
    # Did we get a submitted form?
    request_attempted = False
    request_valid = False
    reason_bad = ''
    formdata = util.FieldStorage(req)
    password = None
    again = None
    if len(formdata.keys()) > 0:
        request_attempted = True
        if 'password' in formdata.keys():
            password = formdata['password'].value
        if 'again' in formdata.keys():
            again = formdata['again'].value

        # Validate
        if password is None:
            reason_bad = 'No new Password supplied'
        elif password != again:
            reason_bad = 'Password and Password again do not match'
        elif bad_password(password):
            reason_bad = 'Bad password - must be at least 14 characters long, and contain at least one lower case letter, upper case letter, decimal digit and non-alphanumeric character (e.g. !, #, %, * etc)'
        else:
            request_valid = True

    if request_valid:
        req.write('<H2>Processing your request...</H2>')
        with session_scope() as session:
            user = session.query(User).get(userid)
            if user.validate_reset_token(token):
                user.reset_password(password)
                session.commit()
                req.write('<P>Password has been reset.</P>')
                req.write('<p><a href="/login">Click here to log in.</a></p>')
                return apache.HTTP_OK
            else:
                req.write("<P>Link is no longer valid. Please request a new one.</P>")
                req.write('</body></html>')
                return apache.HTTP_OK

    if request_attempted:
        req.write("<P>Your request was invalid. %s. Please try again.</P>" % reason_bad)

    # Send the new account form
    req.write('<FORM action="/password_reset/%d/%s" method="POST">' % (userid, token))
    req.write('<P>Fill out and submit this form to reset your password. Password must be at least 14 characters long, and contain at least one lower case letter, upper case letter, decimal digit and non-alphanumeric character (e.g. !, #, %, * etc)</P>')
    req.write('<TABLE>')
    req.write('<TR><TD><LABEL for="password">New Password</LABEL></TD>')
    req.write('<TD><INPUT type="password" size=16 name="password"</TD></TR>')
    req.write('<TR><TD><LABEL for="again">New Password Again</LABEL></TD>')
    req.write('<TD><INPUT type="password" size=16 name="again"</TD></TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Submit"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.HTTP_OK

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
    if things:
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if len(formdata.keys()) > 0:
        request_attempted = True
        if 'oldpassword' in formdata.keys():
            oldpassword = formdata['oldpassword'].value
        if 'newpassword' in formdata.keys():
            newpassword = formdata['newpassword'].value
        if 'newagain' in formdata.keys():
            newagain = formdata['newagain'].value

        # Validate what came in
        valid_request = False

        if oldpassword == '':
            reason_bad = 'No old password supplied'
        elif newpassword == '':
            reason_bad = 'No new password supplied'
        elif newagain == '':
            reason_bad = 'No new password again supplied'
        elif bad_password(newpassword):
            reason_bad = 'Bad password - must be at least 8 characters and contain both uppercase and lowercase letters, and numbers'
        elif newpassword != newagain:
            reason_bad = 'New Password and New Password Again do not match'
        else:
            valid_request = True

    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Password reset request</title></head><body>")

    if valid_request:
        req.write('<H2>Processing your request...</H2>')
        with session_scope() as session:
            user = userfromcookie(session, req)
            if user is None:
                valid_request = False
                reason_bad = 'You are not currently logged in'
            elif user.validate_password(oldpassword) is False:
                valid_request = False
                reason_bad = 'Current password not correct'
            else:
                user.change_password(newpassword)
                session.commit()
                req.write('<p>Password has been changed</p>')
                if things:
                    req.write('<p><a href="/searchform%s">Click here to go back to your searchform</p>' % thing_string)
                else:
                    req.write('<p><a href="/searchform">Click here to go to the searchform</p>')
                sucessfull = True

    if request_attempted is True and valid_request is False:
        req.write('<h2>Request not valid:</h2>')
        req.write('<p>%s</p>' % reason_bad)

    if not sucessfull:
        # Send the password change form
        req.write('<FORM action="/change_password%s" method="POST">' % thing_string)
        req.write('<P>Fill out and submit this form to change your password. Password must be 8 characters or more and must contain uppercase and lowercase letters, and numbers.</P>')
        req.write('<TABLE>')
        req.write('<TR><TD><LABEL for="oldpassword">Current Password</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="oldpassword"</TD></TR>')
        req.write('<TR><TD><LABEL for="newpassword">New Password</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="newpassword"</TD></TR>')
        req.write('<TR><TD><LABEL for="newagain">New Password Again</LABEL></TD>')
        req.write('<TD><INPUT type="password" size=16 name="newagain"</TD></TR>')
        req.write('</TABLE>')
        req.write('<INPUT type="submit" value="Submit"></INPUT>')
        req.write('</FORM>')


    req.write("</body></html>")
    return apache.HTTP_OK

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
    if len(formdata.keys()) > 0:
        if 'thing' in formdata.keys():
            thing = formdata['thing'].value

        # Validate
        if username_inuse(thing):
            # They gave us a valid username
            username = thing
            request_valid = True
        elif email_inuse(thing):
            # They gave us a valid email address
            email = thing
            request_valid = True
        else:
            request_valid = False
            req.write('<P>That is not a valid username or email address in our system. Maybe you need to <a href="/request_account">create a new account</a>?</P>')

    if request_valid:
        # Try to process it
        req.write('<P>Processing request...</P>')
        with session_scope() as session:
            query = session.query(User)
            if username:
                query = query.filter(User.username == username)
            elif email:
                query = query.filter(User.email == email)
            else:
                # Something is wrong
                req.write('<P>Error: no valid username or email. This should not happen.</P>')
                return apache.HTTP_OK
            users = query.all()
            if len(users) > 1:
                req.write("<P>Multiple usernames are using this email address. We'll send an email for each username. Please contact the Gemini Helpdesk to sort this out.</P>")
            for user in users:
                req.write("<P>Sending password reset email for username: %s</P>" % user.username)
                emailed = send_password_reset_email(user.id)
                if emailed:
                    req.write('<P>You should shortly receive an email with a link to set your password and activate your account.</P>')
                    req.write("<P>If you don't get the email, please contact the Gemini helpdesk.</P>")
                else:
                    req.write('<P>Sending you a password reset email FAILED. Please contact Gemini Helpdesk. Sorry.</P>')

        req.write('</body></html>')
        return apache.HTTP_OK

    # Send the reset form
    req.write('<FORM action="/request_password_reset" method="POST">')
    req.write("<P>Enter your Gemini Archive Username or the Email address you registered with us when you created the account in the box below and hit submit. We'll send you an email containing a link to follow to reset your password. Please note that the link is only valid for 15 minutes.</P>")
    req.write('<INPUT type="text" size=32 name="thing"</INPUT>')

    # Some kind of captcha here.

    req.write('<INPUT type="submit" value="Submit"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.HTTP_OK

def staff_access(req, things):
    """
    Allows supersusers to set accounts to be or not be gemini staff
    """
    # Process the form data first if there is any
    formdata = util.FieldStorage(req)
    username = ''
    action = ''

    # Parse the form data
    if len(formdata.keys()) > 0:
        if 'username' in formdata.keys():
            username = formdata['username'].value
        if 'action' in formdata.keys():
            action = formdata['action'].value

    req.content_type = 'text/html'
    req.write("<html><head><title>Gemini Archive Staff Access</title>")
    req.write('<link rel="stylesheet" href="/table.css">')
    req.write("</head><body>")
    req.write('<h1>Gemini Archive Staff Access</h1>')

    with session_scope() as session:
        thisuser = userfromcookie(session, req)
        if thisuser is None or thisuser.superuser != True:
            req.write("<p>You don't appear to be logged in as a superuser. Sorry.</p>")
            req.write('</body></html>')
            return apache.HTTP_OK

        # If we got an action, do it
        if username:
            try:
                user = session.query(User).filter(User.username == username).one()
                if action == "Grant":
                    action_name = 'Granting'
                    user.gemini_staff = True
                elif action == "Revoke":
                    action_name = 'Revoking'
                    user.gemini_staff = False
                req.write("<p>%s staff access for username: %s - %s - %s</p>" % (action_name, user.username, user.fullname, user.email))
                session.commit()
            except NoResultFound:
                req.write("<p>Could not locate user in database</p>")

        # Have applied changes, now generate list of staff users
        query = session.query(User).order_by(User.gemini_staff, User.username)
        staff_users = query.all()

        even = False
        req.write('<TABLE>')
        req.write('<TR class=tr_head><TH>Username</TH><TH>Full Name</TH><TH>Email</TH><TH>Staff Access</TH><TH>Superuser</TH><TR>')
        for user in staff_users:
            even = not even
            row_class = "tr_even" if even else "tr_odd"
            req.write('<TR class=%s><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD></TR>' % (row_class, user.username, user.fullname, user.email, user.gemini_staff, user.superuser))
        req.write('</TABLE>')

    req.write('<H2>Grant or Revoke Staff Access</H2>')
    req.write('<FORM action="/staff_access" method="POST">')
    req.write('<label for="username">Username:</label><input type="text" name="username">')
    req.write('<select name="action"><option value="">Action</option><option value="Grant">Grant</option><option value="Revoke">Revoke</option></select>')
    req.write('<INPUT type="submit" value="Submit"></INPUT>')
    req.write('</FORM>')
    req.write('</body></html>')

    return apache.HTTP_OK

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
    if things:
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    # Parse the form data here
    if len(formdata.keys()) > 0:
        request_attempted = True
        if 'username' in formdata.keys():
            username = formdata['username'].value
        if 'password' in formdata.keys():
            password = formdata['password'].value

        # Validate
        valid_request = False
        if username == '':
            reason_bad = "No Username supplied"
        elif password == '':
            reason_bad = "No Password supplied"
        elif not username_inuse(username):
            reason_bad = "Username / password not valid"
        else:
            # Find the user and check if the password is valid
            with session_scope() as session:
                user = session.query(User).filter(User.username == username).one()
                if user.validate_password(password):
                    # Sucessfull login
                    cookie = user.log_in()
                    valid_request = True
                else:
                    reason_bad = 'Username / password not valid. If you need to reset your password, <a href="/request_password_reset">Click Here</a>'

    req.content_type = "text/html"
    if valid_request:
        # Cookie expires in 1 year
        cookie_obj = Cookie.Cookie('gemini_archive_session', cookie, expires=time.time()+31536000, path="/")
        Cookie.add_cookie(req, cookie_obj)

    req.write("<html><head><title>Gemini Archive log in</title></head><body>")

    if valid_request:
        req.write('<P>Welcome, you are sucessfully logged in to the Gemini Archive.</P>')
        if things:
            url = "/searchform"
            for thing in things:
                url += "/%s" % thing
            req.write('<p><a href="%s">Click here to go back to your search form</a></p>' % url)
            req.write('<p><a href="/searchform">Click here to go to an empty search form</a></p>')
        else:
            req.write('<p><a href="/searchform">Click here to go to the search form</a></p>')

        req.write('</body></html>')
        return apache.HTTP_OK

    if request_attempted:
        req.write('<P>Log-in did not suceed: %s. Please try again.</P>' % reason_bad)
        req.write('<P>If you have forgotten your username and/or password, <a href="/request_password_reset">click here to reset your password</a>.</P>')

    req.write('<FORM action="/login%s" method="POST">' % thing_string)
    req.write('<TABLE>')
    req.write('<TR><TD><LABEL for="username">Username</LABEL><TD>')
    req.write('<TD><INPUT type="text" size=16 name="username" value=%s></INPUT></TD></TR>' % username)
    req.write('<TR><TD><LABEL for="password">Password</LABEL><TD>')
    req.write('<TD><INPUT type="password" size=16 name="password"></INPUT></TD></TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Submit"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.HTTP_OK


def logout(req):
    """
    Log out all archive sessions for this user
    """
    # Do we have a session cookie?
    cookie = None
    cookies = Cookie.get_cookies(req)
    if cookies.has_key('gemini_archive_session'):
        cookie = cookies['gemini_archive_session'].value

    if cookie:
        # Find the user that we are
        with session_scope() as session:
            users = session.query(User).filter(User.cookie == cookie).all()

# This if does nothing...
#            if len(users) == 0:
#                # We weren't really logged in.
#                pass
            if len(users) > 1:
                # Eeek, multiple users with the same session cookie!?!?!
                req.log_error("Logout - Multiple Users with same session cookie: %s" % cookie)
            for user in users:
                user.log_out_all()

        Cookie.add_cookie(req, 'gemini_archive_session', '', expires=time.time())
    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive log out</title></head><body>")
    req.write('<P>You are sucessfully logged out of the Gemini Archive.</P>')
    req.write('<p>You might now want to go to the <a href="/">archive home page</a></p>')
    req.write('</body></html>')
    return apache.HTTP_OK

def whoami(req, things):
    """
    Tells you who you are logged in as, and presents the account maintainace links
    """
    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Who Am I</title>")
    #req.write('<link rel="stylesheet" type="text/css" href="/whoami.css">')
    req.write("</head><body>")

    username = None
    # Find out who we are if logged in
    with session_scope() as session:
        user = userfromcookie(session, req)

        if user is not None:
            username = user.username
            fullname = user.fullname

    # Construct the "things" part of the URL for the link that want to be able to
    # take you back to the same form contents
    if len(things):
        thing_string = '/' + '/'.join(things)
    else:
        thing_string = ''

    req.write('<span id="whoami">')
    if username:
        # Unicode &#x1f464 is preferable to the user icon, if only browsers supported it (mid 2014)
        req.write('<img src="/user1-64x64.png" height="18px"> %s &#9662' % username)
        req.write('<ul class="whoami">')
        req.write('<li class="whoami">%s</li>' % fullname)
        req.write('<li class="whoami"><a href="/logout">Log Out</a></li>')
        req.write('<li class="whoami"><a href="/change_password%s">Change Password</a></li>' % thing_string)
        req.write('<li class="whoami"><a href="/my_programs%s">My Programs</a></li>' % thing_string)
    else:
        req.write('<img src="/user1-64x64.png" height="18px"> Not logged in &#9662')
        req.write('<ul class="whoami">')
        req.write('<li class="whoami"><a href="/request_account%s">Request Account</a></li>' % thing_string)
        req.write('<li class="whoami"><a href="/login%s">Login</a></li>' % thing_string)
        req.write('</ul>')

    req.write('</span>')
    req.write('</body></html>')
    return apache.HTTP_OK

def user_list(req):
    """
    Displays a list of archive users. Must be logged in as a gemini_staff user to
    see this.
    """
    req.content_type = 'text/html'
    req.write("<html><head><title>Gemini Archive User List</title>")
    req.write('<link rel="stylesheet" href="/table.css">')
    req.write("</head><body>")
    req.write('<h1>Gemini Archive User List</h1>')

    with session_scope() as session:
        thisuser = userfromcookie(session, req)
        if thisuser is None or thisuser.gemini_staff != True:
            req.write("<p>You don't appear to be logged in as a Gemini Staff user. Sorry.</p>")
            req.write('</body></html>')
            return apache.HTTP_OK

        query = session.query(User).order_by(desc(User.superuser)).order_by(desc(User.gemini_staff))
        query = query.order_by(User.username)
        users = query.all()

        even = False
        req.write('<TABLE>')
        req.write('<TR class=tr_head><TH>Username</TH><TH>Full Name</TH><TH>Email</TH><TH>Password</TH><TH>Staff Access</TH><TH>Superuser</TH><TH>Reset Requested</TH><TH>Reset Active</TH><TH>Account Create</TH><TH>Last Password Change</TH><TR>')
        for user in users:
            even = not even
            row_class = "tr_even" if even else "tr_odd"
            password = user.password is not None
            reset_requested = user.reset_token is not None
            reset_active = (user.reset_token is not None) and (user.reset_token_expires > datetime.datetime.utcnow())
            req.write('<TR class=%s><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD></TR>' % (row_class, user.username, user.fullname, user.email, password, user.gemini_staff, user.superuser, reset_requested, reset_active, user.account_created, user.password_changed))
    req.write('</TABLE>')

    req.write('</body></html>')

    return apache.HTTP_OK


def email_inuse(email):
    """
    Check the database to see if this email is already in use. Returns True if it is, False otherwise
    """

    with session_scope() as session:
        num = session.query(User).filter(User.email == email).count()

        return num != 0

def username_inuse(username):
    """
    Check the database to see if a username is already in use. Returns True if it is, False otherwise
    """

    with session_scope() as session:
        num = session.query(User).filter(User.username == username).count()

        return num != 0

digits_cre = re.compile(r'\d')
lower_cre = re.compile('[a-z]')
upper_cre = re.compile('[A-Z]')
nonalpha_cre = re.compile('[^a-zA-Z0-9]')
password_criteria = (digits_cre, lower_cre, upper_cre, nonalpha_cre)
def bad_password(candidate):
    """
    Checks candidate for compliance with password rules.
    Returns True if it is bad, False if it is good
    """

    all_crit = all(x.search(candidate) for x in password_criteria)
    if len(candidate) > 13 and all_crit:
        return False

    return True

def userfromcookie(session, req):
    """
    Given a database session and request object, get the session cookie
    from the request object and find and return the user object,
    or None if it is not a valid session cookie
    """

    # Do we have a session cookie?
    cookie = None
    cookies = Cookie.get_cookies(req)
    if cookies.has_key('gemini_archive_session'):
        cookie = cookies['gemini_archive_session'].value
    else:
        # No session cookie, not logged in
        return None

    # Find the user that we are
    try:
        return session.query(User).filter(User.cookie == cookie).one()
    except NoResultFound:
        # This is not a valid session cookie
        return None
