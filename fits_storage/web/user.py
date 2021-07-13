"""
This module handles the web 'user' functions - creating user accounts, login / logout, password reset etc
"""
import urllib

import requests
from urllib.parse import urlencode

from sqlalchemy import desc

from ..orm import NoResultFound
from ..orm.user import User

from ..utils.web import get_context, Return

from ..fits_storage_config import fits_servername, smtp_server, use_as_archive, orcid_client_id, orcid_client_secret, \
    orcid_server, orcid_enabled, orcid_redirect_url

from . import templating

import re
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import functools

import logging

bad_password_msg = "Bad password - must be at least 14 characters long, and contain at least one lower case letter, upper case letter, decimal digit and non-alphanumeric character (e.g. !, #, %, * etc)"


@templating.templated("user/request_account.html")
def request_account(things):
    """
    Generates and handles web form for requesting new user accounts
    """

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
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
        elif email_inuse(email):
            reason_bad = "Email already registered"
        else:
            valid_request = True

    template_args = dict(
        reason_bad=reason_bad,
        request_attempted=request_attempted,
        # Contruct the thing_string for the url to link back to their form
        thing_string='/'.join(things),
        valid_request=valid_request,
        # User data
        username=username,
        fullname=fullname,
        email=email,
        maybe_gemini=email.endswith("@gemini.edu"),
        # For debugging
        debugging=False,
        formdata=formdata
    )

    if valid_request:
        try:
            newuser = User(username)
            newuser.fullname = fullname
            newuser.email = email
            session = ctx.session
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

    message_html = """\
<html><head></head><body>
<p>Hello {name},</p>
<p>A password reset has been requested for the Gemini Archive account 
registered to this email address. If you did not request a password reset, 
you can safely ignore this email, though if you get several spurious reset 
request emails, please file a helpdesk ticket at 
<a href="http://www.gemini.edu/sciops/helpdesk">http://www.gemini.edu/sciops/helpdesk</a>
in the Gemini Observatory Archive category to let us know. Assuming that you 
requested this password reset, please click on the link below or paste it into 
your browser to reset your password. The reset link is only valid for 15 minutes, 
so please do that promptly.</p>
<p>The username for this account is {username}</p>
<p><a href="{url}">{url}</a></p>
<p>Regards,</p>
<p>Gemini Observatory Archive</p>
</body></html>
"""

    user = get_context().session.query(User).get(userid)
    username = user.username
    email = user.email
    fullname = user.fullname
    token = user.generate_reset_token()

    url = "https://%s/password_reset/%d/%s" % (fits_servername, userid, token)

    plaintext = message_text.format(name=fullname, username=username, url=url)
    htmltext = message_html.format(name=fullname, username=username, url=url)

    fromaddr = 'fitsadmin@gemini.edu'
    tolist = [email, fromaddr]
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Gemini Archive Password Reset link'
    msg['From'] = fromaddr
    msg['To'] = email
    part1 = MIMEText(plaintext, 'plain')
    part2 = MIMEText(htmltext, 'html')
    msg.attach(part1)
    msg.attach(part2)

    try:
        smtp = smtplib.SMTP(smtp_server)
        smtp.sendmail(fromaddr, tolist, msg.as_string())
    except:
        return False

    return True


@templating.templated("user/password_reset.html")
def password_reset(userid, token):
    """
    Handles users clicking on a password reset link that we emailed them.
    Check the reset token for validity, if valid the present them with a
    password reset form and process it when submitted.
    """

    ctx = get_context()
    session = ctx.session

    template_args = dict(
        valid_request=False,
    )

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
    formdata = ctx.get_form_data()
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
        userid=userid,
        token=token
    ))

    return template_args


@templating.templated("user/change_email.html")
def change_email(things):
    """
    Handles a logged in user wanting to change their email.
    """
    # Present and process a change email form. User must be logged in.

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    request_attempted = False
    valid_request = None
    reason_bad = None
    successful = False

    newemail = ''
    newagain = ''

    # Parse the form data here
    if formdata:
        request_attempted = True
        if 'newemail' in formdata:
            newemail = formdata['newemail'].value
        if 'newagain' in formdata:
            newagain = formdata['newagain'].value

        # Validate what came in
        valid_request = False

        if newemail == '':
            reason_bad = 'No new email supplied'
        elif newagain == '':
            reason_bad = 'No new email again supplied'
        elif ('@' not in newemail) or ('.' not in newemail):
            reason_bad = "Not a valid Email address"
        elif ',' in newemail:
            reason_bad = "Email address cannot contain commas"
        elif email_inuse(newemail):
            reason_bad = "Email address is already in use"
        elif newemail != newagain:
            reason_bad = 'New Email and New Email Again do not match'
        else:
            valid_request = True

    if valid_request:
        user = ctx.user
        if user is None:
            valid_request = False
            reason_bad = 'You are not currently logged in'
        else:
            user.email = newemail
            ctx.session.commit()
            successful = True

    template_args = dict(
        successful=successful,
        reason_bad=reason_bad,
        # Construct the things_string to link back to the current form
        thing_string='/'.join(things)
    )

    return template_args


@templating.templated("user/change_password.html")
def change_password(things):
    """
    Handles a logged in user wanting to change their password.
    """
    # Present and process a change password form. User must be logged in,
    # and know their current password.

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
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
        user = ctx.user
        if user is None:
            valid_request = False
            reason_bad = 'You are not currently logged in'
        elif not user.username:
            valid_request = False
            reason_bad = 'This account has no password based login'
        elif user.validate_password(oldpassword) is False:
            valid_request = False
            reason_bad = 'Current password not correct'
        else:
            user.change_password(newpassword)
            ctx.session.commit()
            successful = True

    template_args = dict(
        successful=successful,
        reason_bad=reason_bad,
        # Construct the things_string to link back to the current form
        thing_string='/'.join(things)
    )

    return template_args


@templating.templated("user/request_password_reset.html")
def request_password_reset():
    """
    Generate and process a web form to request a password reset
    """

    ctx = get_context()

    # Process the form data first if thre is any
    formdata = ctx.get_form_data()
    request_valid = None

    username = None
    orcid = None
    email = None

    # Parse the form data here
    if formdata:
        if 'thing' in list(formdata.keys()):
            thing = formdata['thing'].value

        # Validate
        if username_inuse(thing):
            # They gave us a valid username
            username = thing
            request_valid = True
        elif orcid_inuse(thing):
            orcid = thing
            request_valid = True
        elif email_inuse(thing):
            # They gave us a valid email address
            email = thing
            request_valid = True
        else:
            request_valid = False

    template_args = dict(
        request_valid=request_valid
    )

    if request_valid:
        # Try to process it
        query = ctx.session.query(User)
        if username:
            query = query.filter(User.username == username)
        elif orcid:
            query = query.filter(User.orcid_id == orcid)
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


@templating.templated("user/staff_access.html")
def staff_access():
    """
    Allows supersusers to set accounts to be or not be gemini staff
    """

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    username = ''
    action = ''

    # Parse the form data
    if formdata:
        if 'username' in list(formdata.keys()):
            username = formdata['username'].value
        if 'action' in list(formdata.keys()):
            action = formdata['action'].value

    thisuser = ctx.user
    if thisuser is None or thisuser.superuser != True:
        return dict(allowed=False)

    template_args = dict(allowed=True)

    # If we got an action, do it
    if username:
        try:
            user = ctx.session.query(User).filter(User.username == username).one()
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
    template_args['user_list'] = ctx.session.query(User).order_by(User.gemini_staff, User.username)

    return template_args


@templating.templated("user/admin_change_email.html")
def admin_change_email():
    """
    Allows supersusers to set emails on user accounts
    """

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    username = ''
    email = ''
    action = ''

    # Parse the form data
    if formdata:
        if 'username' in list(formdata.keys()):
            username = formdata['username'].value
        if 'email' in list(formdata.keys()):
            email = formdata['email'].value

    # Permission requires either superuser or user_admin
    thisuser = ctx.user
    if thisuser is None or (thisuser.superuser is not True and thisuser.user_admin is not True):
        return dict(allowed=False)

    template_args = dict(allowed=True)
    template_args['user_list'] = ctx.session.query(User).order_by(User.gemini_staff, User.username)
    if email and email_inuse(email):
        template_args['email_in_use'] = True
        return template_args
    if email and (('@' not in email) or ('.' not in email) or (',' in email)):
        template_args['email_invalid'] = "Not a valid Email address"
        return template_args

    # If we got an action, do it
    if username:
        try:
            user = ctx.session.query(User).filter(User.username == username).one()
            user.email = email
            template_args['email_changed'] = True
            template_args['action_user'] = user
        except NoResultFound:
            template_args['no_result'] = True

    # Have applied changes, now generate list of staff users
    template_args['user_list'] = ctx.session.query(User).order_by(User.gemini_staff, User.username)

    ctx.session.commit()

    return template_args


@templating.templated("user/login.html")
def login(things):
    """
    Presents and processes a login form
    Sends session cookie if sucessfull
    """
    ctx = get_context()

    redirect = None
    try:
        qs = ctx.env.qs
        if qs and qs.startswith('redirect='):
            redirect = qs[9:]
    except KeyError:
        pass  # no query string, that's ok and redirect is set to None

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    request_attempted = False
    valid_request = None
    reason_bad = None

    username = ''
    password = ''
    redirect = ''
    cookie = None

    # Parse the form data here
    if formdata:
        request_attempted = True
        if 'username' in formdata:
            username = formdata['username'].value
        if 'password' in formdata:
            password = formdata['password'].value
        if 'redirect' in formdata:
            redirect = formdata['redirect'].value

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
            user = ctx.session.query(User).filter(User.username == username).one()
            if user.validate_password(password):
                # Sucessfull login
                cookie = user.log_in()
                valid_request = True
            else:
                reason_bad = 'Username / password not valid. If you need to reset your password, <a href="/request_password_reset">Click Here</a>'

    if valid_request:
        # Cookie expires in 1 year
        exp = datetime.datetime.utcnow() + datetime.timedelta(seconds=31536000)
        ctx.cookies.set('gemini_archive_session', cookie, expires=exp, path="/")
        if redirect:
            ctx.resp.redirect_to(redirect)

    template_args = dict(
        # Rebuild the thing_string for the url
        thing_string='/'.join(things),
        valid_request=valid_request,
        reason_bad=reason_bad,
        username=username,
        redirect=redirect,
    )

    return template_args


@templating.templated("user/logout.html")
def logout():
    """
    Log out all archive sessions for this user
    """
    # Do we have a session cookie?
    ctx = get_context()

    try:
        cookie = ctx.cookies['gemini_archive_session']

        # Find the user that we are
        users = ctx.user
        if isinstance(users, User):
            users = [users]

        if len(users) > 1:
            # Eeek, multiple users with the same session cookie!?!?!
            ctx.log("Logout - Multiple Users with same session cookie: %s" % cookie)
        for user in users:
            user.log_out_all()

        del ctx.cookies['gemini_archive_session']
    except KeyError:
        # There was no cookie
        pass

    return {}


@templating.templated("user/whoami.html")
def whoami(things):
    """
    Tells you who you are logged in as, and presents the account maintainace links
    """
    # Find out who we are if logged in

    template_args = {'orcid_enabled': orcid_enabled}

    user = get_context().user

    try:
        template_args['username'] = user.username if user.username else ""
        template_args['orcid'] = user.orcid_id if user.orcid_id else ""
        template_args['fullname'] = user.fullname
        template_args['is_superuser'] = user.superuser
        template_args['user_admin'] = user.user_admin
    except AttributeError:
        # no user
        pass

    # Construct the "things" part of the URL for the link that want to be able to
    # take you back to the same form contents
    template_args['thing_string'] = '/'.join(things)

    return template_args


@templating.templated("user/list.html")
def user_list():
    """
    Displays a list of archive users. Must be logged in as a gemini_staff user to
    see this.
    """

    ctx = get_context()

    thisuser = ctx.user
    if thisuser is None or thisuser.gemini_staff != True:
        return dict(staffer=False)

    users = (ctx.session.query(User)
             .order_by(desc(User.superuser),
                       desc(User.gemini_staff),
                       User.username))

    return dict(staffer=True,
                users=users)


def email_inuse(email):
    """
    Check the database to see if this email is already in use. Returns True if it is, False otherwise.

    Email addresses are case-insensitive, so we have to get clever here to do a case insensitive
    match.  Also, in future we can add a constraint to the database.  However, currently we already
    have existing users with duplicate emails.  So, until we have a strategy for how to unwind those
    accounts we can't constrain the database.  A trigger would be another option, but we'd still want
    to catch it here to alert the user.  I'm inclined to just fix the account data and then add a
    proper unique constraint and skip doing any kind of trigger validation.
    """
    rows = get_context().session.execute("select count(1) from archiveuser where LOWER(email)=:email",
                                         {'email': email.lower()})
    for row in rows:
        if len(row) > 0 and row[0] == 0:
            return False
    return True


def username_inuse(username):
    """
    Check the database to see if a username is already in use. Returns True if it is, False otherwise
    """

    num = get_context().session.query(User).filter(User.username == username).count()
    return num != 0


def orcid_inuse(orcid):
    """
    Check the database to see if a username is already in use. Returns True if it is, False otherwise
    """

    num = get_context().session.query(User).filter(User.orcid_id == orcid).count()
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


def needs_login(magic_cookies=(), only_magic=False, staffer=False, misc_upload=False, superuser=False,
                content_type='html', annotate=None, archive_only=False):
    """Decorator for functions that need a user to be logged in, or some sort of cookie
       to be set. The basic use is (notice the decorator parenthesis, they're important)::

           @needs_login()
           def decorated_function():
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
        def wrapper(*args, **kw):
            logging.debug("Checking authorization")
            ctx = get_context()

            ctype = 'application/json' if content_type == 'json' else 'text/html'

            disabled_cookies = any(not expected for cookie, expected in magic_cookies)

            got_magic = False
            if disabled_cookies and only_magic:
                logging.debug("No cookies, only magic")
                got_magic = True
            elif not disabled_cookies:
                for cookie, content in magic_cookies:
                    try:
                        if content is not None and ctx.cookies[cookie] == content:
                            logging.debug("Saw magic cookie")
                            got_magic = True
                            break
                    except KeyError:
                        pass

            if archive_only and not use_as_archive:
                logging.debug("Archive only and not use as archive")
                # Bypass protection - archive_only and not the archive
                got_magic = True
            if not got_magic:
                logging.debug("Handling auth")
                raise_error = functools.partial(ctx.resp.client_error, code=Return.HTTP_FORBIDDEN, content_type=ctype,
                                                annotate=annotate)
                if only_magic:
                    logging.info("Could not find a proper magic cookie for a cookie-only service")
                    raise_error(message="Could not find a proper magic cookie for a cookie-only service")
                user = ctx.user
                if not user:
                    logging.info("You need to be logged in to access this resource")
                    raise_error(message="You need to be logged in to access this resource")
                if superuser is True and not user.superuser:
                    logging.info("You need to be logged in as a Superuser to access this resource")
                    raise_error(message="You need to be logged in as a Superuser to access this resource")
                if staffer is True and not user.gemini_staff:
                    logging.info("You need to be logged in as Gemini Staff member to access this service")
                    raise_error(message="You need to be logged in as Gemini Staff member to access this resource")
                if misc_upload is True and (not user.misc_upload and not user.superuser):
                    logging.info("You need to be logged in with misc upload permission (or as a superuser) to access "
                                 "this service")
                    raise_error(message="You need to be logged in with misc upload permission or as a supersuer to "
                                        "access this service")
            logging.debug("Past auth check, calling method")
            return fn(*args, **kw)

        return wrapper

    return decorator


@templating.templated("user/orcid.html")
def orcid(code):
    """
    Generates and handles ORCID backed accounts

    ``code``
      This is the ORCID supplied authentication code.  We can use this to request
      the users identity from the ORCID service.  On the first pass to this
      endpoint, there is no code and we redirect the user to the ORCID login page.
    """

    if not orcid_enabled:
        return dict(
            notification_message="",
            reason_bad="ORCID not enabled on this system"
        )

    notification_message = ""
    reason_bad = ""

    redirect_url = orcid_redirect_url

    ctx = get_context()

    if code:
        # Need to POST the token to ORCID to get the credentials
        data = {
            "client_id": orcid_client_id,
            "client_secret": orcid_client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_url
        }
        orcid_token_url = 'https://%s/oauth/token' % orcid_server
        r = requests.post(orcid_token_url, data=data)
        if r.status_code == 200:
            response_data = r.json()
            orcid_id = response_data["orcid"]
            # create a session for this orcid id
            user = ctx.session.query(User).filter(User.orcid_id == orcid_id).one_or_none()
            if user is None:
                # Authorized as ORCID user and we haven't seen this ORCID before
                if ctx.user:
                    # Add ORCID to existing user
                    ctx.user.orcid_id = orcid_id
                    ctx.session.save(ctx.user)
                else:
                    # make a new user record with our ORCID data
                    user = User('')
                    user.orcid_id = orcid_id
                    user.fullname = response_data['name']
                    session = ctx.session
                    session.add(user)
                    session.commit()

            cookie = user.log_in()
            exp = datetime.datetime.utcnow() + datetime.timedelta(seconds=31536000)
            ctx.cookies.set('gemini_archive_session', cookie, expires=exp, path="/")
            ctx.resp.redirect_to('/searchform')  # 'http://localhost:8090/searchform')
        else:
            reason_bad = "Error communicating with ORCID service"
    else:
        # Send them to ORCID, which will callback here with their token
        orcid_url = 'https://%s/oauth/authorize?client_id=%s&' \
                    'response_type=code&scope=/authenticate&redirect_uri=%s' \
                    % (orcid_server, orcid_client_id, urllib.parse.quote(redirect_url))

        ctx.resp.redirect_to(orcid_url)

    template_args = dict(
        notification_message=notification_message,
        reason_bad=reason_bad
    )

    return template_args
