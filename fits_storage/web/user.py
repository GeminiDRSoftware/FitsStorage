"""
This module handles the web 'user' functions - creating user accounts, login / logout, password reset etc
"""

from sqlalchemy import desc

from ..orm import session_scope, NoResultFound
from ..orm.user import User

from ..fits_storage_config import fits_servername, smtp_server, use_as_archive

from . import templating

# This will only work with apache
from mod_python import apache
from mod_python import Cookie
from mod_python import util

import re
import datetime
import time
import smtplib
from email.mime.text import MIMEText
import functools

bad_password_msg = "Bad password - must be at least 14 characters long, and contain at least one lower case letter, upper case letter, decimal digit and non-alphanumeric character (e.g. !, #, %, * etc)"

@templating.templated("user/request_account.html", with_session=True)
def request_account(session, req, things):
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

    # Parse the form data here
    if formdata:
        request_attempted = True
        if 'username' in formdata:
            username = formdata['username'].value
        if 'fullname' in formdata:
            fullname = formdata['fullname'].value
        if 'email' in formdata:
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

    template_args = dict(
        reason_bad        = reason_bad,
        request_attempted = request_attempted,
        # Contruct the thing_string for the url to link back to their form
        thing_string      = '/'.join(things),
        valid_request     = valid_request,
        # User data
        username          = username,
        fullname          = fullname,
        email             = email,
        maybe_gemini      = email.endswith("@gemini.edu"),
        # For debugging
        debugging         = False,
        formdata          = formdata
        )

    if valid_request:
        try:
            with session_scope() as session:
                newuser = User(username)
                newuser.fullname = fullname
                newuser.email = email
                session.add(newuser)
                session.commit()
                template_args['emailed'] = send_password_reset_email(newuser.id)
        except:
            template_args['error'] = True

    return template_args

def send_password_reset_email(userid):
    """
    Sends the user a password reset email
    """

    message_text = """\
Hello {name},

  A password reset has been requested for the Gemini Archive account 
registered to this email address. If you did not request a password reset, 
you can safely ignore this email, though if you get several spurious reset 
request emails, please file a helpdesk ticket at 
http://www.gemini.edu/sciops/helpdesk in the Gemini Observatory Archive
category to let us know. Assuming that you requested this password reset, 
please click on the link below or paste it into your browser to reset your 
password. The reset link is only valid for 15 minutes, so please do that 
promptly.

The username for this account is {username}
{url}

Regards,
    Gemini Observatory Archive

"""

    with session_scope() as session:
        user = session.query(User).get(userid)
        username = user.username
        email = user.email
        fullname = user.fullname
        token = user.generate_reset_token()

    url = "https://%s/password_reset/%d/%s" % (fits_servername, userid, token)

    message = message_text.format(name=fullname, username=username, url=url)

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

@templating.templated("user/password_reset.html", with_session=True)
def password_reset(session, req, things):
    """
    Handles users clicking on a password reset link that we emailed them.
    Check the reset token for validity, if valid the present them with a
    password reset form and process it when submitted.
    """

    template_args = dict(
        valid_request = False,
        debugging     = False, # Activate to see in the page what things are we passed
        things        = things
        )

    if len(things) != 2:
        return template_args

    # Extract and validate the things from the URL
    userid = things[0]
    token = things[1]

    try:
        userid = int(userid)
    except:
        return template_args

    if len(token) != 56:
        return template_args

    # OK, seems possibly legit. Check with database
    user = session.query(User).get(userid)
    try:
        if user is None:
            return template_args
        elif user.reset_token_expires < datetime.datetime.utcnow():
            template_args['expired'] = True
            return template_args
        elif user.reset_token != token:
            template_args['invalid_token'] = True
            return template_args
    except TypeError:
        # Probably something was None - from them trying to re-use the link
        # in the email
        template_args['invalid_token'] = True
        return template_args

    # If we got this far we have a valid request.
    template_args['valid_request'] = True
    # Did we get a submitted form?
    request_attempted = False
    request_valid = False
    formdata = util.FieldStorage(req)
    password = None
    again = None
    if formdata:
        request_attempted = True
        if 'password' in formdata:
            password = formdata['password'].value
        if 'again' in formdata:
            again = formdata['again'].value

        # Validate
        if password is None:
            template_args['reason_bad'] = 'No new Password supplied'
        elif password != again:
            template_args['reason_bad'] = 'Password and Password again do not match'
        elif bad_password(password):
            template_args['reason_bad'] = bad_password_msg
        else:
            request_valid = True

    if request_valid:
        if user.validate_reset_token(token):
            user.reset_password(password)
            session.commit()
            template_args['password_reset'] = True
            return template_args
        else:
            template_args['invalid_link'] = True
            return template_args

    # Send the new account form
    template_args.update(dict(
        userid = userid,
        token = token
        ))

    return template_args

@templating.templated("user/change_password.html")
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
    successful = False

    oldpassword = ''
    newpassword = ''
    newagain = ''

    # Parse the form data here
    if formdata:
        request_attempted = True
        if 'oldpassword' in formdata:
            oldpassword = formdata['oldpassword'].value
        if 'newpassword' in formdata:
            newpassword = formdata['newpassword'].value
        if 'newagain' in formdata:
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
            reason_bad = bad_password_msg
        elif newpassword != newagain:
            reason_bad = 'New Password and New Password Again do not match'
        else:
            valid_request = True

    if valid_request:
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
                successful = True

    template_args = dict(
        successful    = successful,
        reason_bad    = reason_bad,
        # Construct the things_string to link back to the current form
        thing_string  = '/'.join(things)
        )

    return template_args

@templating.templated("user/request_password_reset.html", with_session=True)
def request_password_reset(session, req):
    """
    Generate and process a web form to request a password reset
    """
    # Process the form data first if thre is any
    formdata = util.FieldStorage(req)
    request_valid = None

    username = None
    email = None

    # Parse the form data here
    if formdata:
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

    template_args = dict(
        request_valid = request_valid
        )

    if request_valid:
        # Try to process it
        query = session.query(User)
        if username:
            query = query.filter(User.username == username)
        elif email:
            query = query.filter(User.email == email)
        else:
            # Something is wrong
            template_args['invalid_data'] = True
            return template_args

        users = query.all()
        if len(users) > 1:
            template_args['multiple_users'] = True
        emailed_users = []
        for user in users:
            emailed = send_password_reset_email(user.id)
            emailed_users.append((user.username, emailed))
        template_args['emailed_users'] = emailed_users

    return template_args

@templating.templated("user/staff_access.html", with_session=True)
def staff_access(session, req, things):
    """
    Allows supersusers to set accounts to be or not be gemini staff
    """
    # Process the form data first if there is any
    formdata = util.FieldStorage(req)
    username = ''
    action = ''

    # Parse the form data
    if formdata:
        if 'username' in formdata.keys():
            username = formdata['username'].value
        if 'action' in formdata.keys():
            action = formdata['action'].value

    thisuser = userfromcookie(session, req)
    if thisuser is None or thisuser.superuser != True:
        return dict(allowed = False)

    template_args = dict(allowed = True)

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
            template_args['action_name'] = action_name
            template_args['action_user'] = user
        except NoResultFound:
            template_args['no_result'] = True

    # Have applied changes, now generate list of staff users
    template_args['user_list'] = session.query(User).order_by(User.gemini_staff, User.username)

    return template_args

@templating.templated("user/login.html")
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

    # Parse the form data here
    if formdata:
        request_attempted = True
        if 'username' in formdata:
            username = formdata['username'].value
        if 'password' in formdata:
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

    template_args = dict(
        # Rebuild the thing_string for the url
        thing_string      = '/'.join(things),
        valid_request     = valid_request,
        reason_bad        = reason_bad,
        username          = username
        )

    return template_args

@templating.templated("user/logout.html")
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

            if len(users) > 1:
                # Eeek, multiple users with the same session cookie!?!?!
                req.log_error("Logout - Multiple Users with same session cookie: %s" % cookie)
            for user in users:
                user.log_out_all()

        Cookie.add_cookie(req, 'gemini_archive_session', '', expires=time.time())

    return {}

@templating.templated("user/whoami.html", with_session=True)
def whoami(session, req, things):
    """
    Tells you who you are logged in as, and presents the account maintainace links
    """
    # Find out who we are if logged in

    template_args = {}

    user = userfromcookie(session, req)

    if user is not None:
        template_args['username'] = user.username
        template_args['fullname'] = user.fullname

    # Construct the "things" part of the URL for the link that want to be able to
    # take you back to the same form contents
    template_args['thing_string'] = '/'.join(things)

    return template_args

@templating.templated("user/list.html", with_session=True)
def user_list(session, req):
    """
    Displays a list of archive users. Must be logged in as a gemini_staff user to
    see this.
    """

    thisuser = userfromcookie(session, req)
    if thisuser is None or thisuser.gemini_staff != True:
        return dict(staffer = False)

    users = (session.query(User)
                .order_by(desc(User.superuser),
                          desc(User.gemini_staff),
                          User.username))

    return dict(staffer = True,
                users   = users)

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

def is_staffer(req, session=None):
    """
    Given a request object and, optionally, a database session, figure out
    if the current logged-in user is a staff member
    """
    try:
        if session is None:
            with session_scope() as s:
                return userfromcookie(s, req).gemini_staff
        else:
            return userfromcookie(session, req).gemini_staff
    except (TypeError, AttributeError):
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

class AccessForbidden(Exception):
    def __init__(self, message, template, content_type='text/html', annotate=None):
        self.message      = message
        self.args         = [message]
        self.template     = template
        self.content_type = content_type
        self.annotate     = annotate

DEFAULT_403_TEMPLATE = 'errors/forbidden.html'
JSON_403_TEMPLATE = 'errors/forbidden.json'

def needs_login(magic_cookies=(), only_magic=False, staffer=False, superuser=False, content_type='html', annotate=None, archive_only=False):
    """Decorator for functions that need a user to be logged in, or some sort of cookie
       to be set. The basic use is (notice the decorator parenthesis, they're important)::

           @needs_login()
           def decorated_function(req):
               ...

       Which rejects access if the user is not logged in. The decorator accepts a number
       of keyword arguments. The most restrictive is the one that applies, except for
       magic cookies:

       ``magic_cookie=[...]``
         A list of cookie ``(name, expected_value)`` pairs. If the query provides a pair that matches any
         of the included ones, even non-logged in users will have granted access. Useful for scripts,
         etc.

       ``only_magic=(False|True)``
         This relates to the **EXPECTED** value of a cookie (the one provided in ``magic_cookie``). If any of
         the expected cookie pairs has an empty (eg. ``None``) value, then the behaviour of ``needs_login``
         is the following:

         * If ``only_magic`` is ``False``, then we revert to the standard auth protocol (we don't check for
           magic cookies at all)
         * If ``only_magic`` is ``True``, and one of the expected values is None, then we allow access always

       ``staffer=(False|True)``
         If ``True``, the user must be Gemini Staff

       ``superuser=(False|True)``
         If ``True``, the user must be Superuser of the Archive system

       ``content_type='...'``
         Can be set to ``'html'`` (the default) or ``'json'``, depending on the kind of answer
         we want to provide, in case that the access is forbidden

       ``archive_only=(False|True)``
         If ``True``, authentication is only required if this is an archive server
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(req, *args, **kw):
            if content_type == 'json':
                ctype    = 'application/json'
                template = JSON_403_TEMPLATE
            else:
                ctype    = 'text/html'
                template = DEFAULT_403_TEMPLATE
            with session_scope() as session:
                disabled_cookies = any(not expected for cookie, expected in magic_cookies)

                got_magic = False
                if disabled_cookies and only_magic:
                    got_magic = True
                elif not disabled_cookies:
                    cookies = Cookie.get_cookies(req)
                    for cookie, content in magic_cookies:
                        try:
                            if content is not None and cookies[cookie].value == content:
                                got_magic = True
                                break
                        except KeyError:
                            pass

                if archive_only and not use_as_archive:
                    # Bypass protection - archive_only and not the archive
                    got_magic = True
                if not got_magic:
                    if only_magic:
                        raise AccessForbidden("Could not find a proper magic cookie for a cookie-only service", template=template, content_type=ctype, annotate=annotate)
                    user = userfromcookie(session, req)
                    if not user:
                        raise AccessForbidden("You need to be logged in to access this resource", template=template, content_type=ctype, annotate=annotate)
                    if superuser is True and not user.superuser:
                        raise AccessForbidden("You need to be logged in as a Superuser to access this resource", template=template, content_type=ctype, annotate=annotate)
                    if staffer is True and not user.gemini_staff:
                        raise AccessForbidden("You need to be logged in as Gemini Staff member to access this resource", template=template, content_type=ctype, annotate=annotate)
            return fn(req, *args, **kw)
        return wrapper
    return decorator
