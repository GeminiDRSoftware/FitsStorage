"""
This module handles the web 'user' functions - creating user accounts,
login / logout, password reset etc
"""

import re
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import functools
import json
import urllib
import requests
from requests.auth import HTTPBasicAuth
import jwt
from urllib.parse import urlencode

from sqlalchemy import desc, and_, or_
from sqlalchemy.exc import NoResultFound

from fits_storage.gemini_metadata_utils import GeminiObservation
from fits_storage.server.orm.user import User
from fits_storage.server.orm.userprogram import UserProgram

from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from . import templating

from fits_storage.config import get_config


bad_password_msg = "Bad password - must be at least 14 characters long, and " \
                   "contain at least one lower case letter, upper case " \
                   "letter, decimal digit and non-alphanumeric character " \
                   "(e.g. !, #, %, * etc)"


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
            username = formdata['username'].value.lower()
        if 'fullname' in formdata:
            fullname = formdata['fullname'].value
        if 'email' in formdata:
            email = formdata['email'].value.lower()

        # Validate
        valid_request = False
        if username == '':
            reason_bad = "No Username supplied"
        elif not username.isalnum():
            reason_bad = "Username may only contain alphanumeric characters"
        elif len(username) < 2:
            reason_bad = "Username too short. Must be at least 2 characters"
        elif username_inuse(username):
            reason_bad = 'Username is already in use, please choose a ' \
                         'different one. If this is your username, you can ' \
                         '<a href="/login">log in</a> if you know your ' \
                         'password, or <a href="/request_password_reset">' \
                         'reset your password</a> if you have forgotten it.'
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
        # Construct the thing_string for the url to link back to their form
        thing_string='/'.join(things),
        valid_request=valid_request,
        # User data
        username=username,
        fullname=fullname,
        email=email,
        maybe_gemini=email.endswith("@gemini.edu") or
                     email.endswith("@noirlab.edu"),
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
        except Exception:
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
<a href="http://www.gemini.edu/sciops/helpdesk">
http://www.gemini.edu/sciops/helpdesk</a>
in the Gemini Observatory Archive category to let us know. Assuming that you 
requested this password reset, please click on the link below or paste it into 
your browser to reset your password. The reset link is only valid for 15 
minutes, so please do that promptly.</p>
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

    fsc = get_config()
    url = "https://%s/password_reset/%d/%s" % (fsc.fits_server_name,
                                               userid, token)

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
        smtp = smtplib.SMTP(fsc.smtp_server)
        smtp.sendmail(fromaddr, tolist, msg.as_string())
    except Exception:
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
    except Exception:
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
    request_valid = False
    formdata = ctx.get_form_data()
    password = None
    again = None
    if formdata:
        if 'password' in formdata:
            password = formdata['password'].value
        if 'again' in formdata:
            again = formdata['again'].value

        # Validate
        if password is None:
            template_args['reason_bad'] = 'No new Password supplied'
        elif password != again:
            template_args['reason_bad'] = \
                'Password and Password again do not match'
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
    Handles a logged-in user wanting to change their email.
    """
    # Present and process a change email form. User must be logged in.

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    valid_request = None
    reason_bad = None
    successful = False

    newemail = ''
    newagain = ''

    # Parse the form data here
    if formdata:
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
    Handles a logged-in user wanting to change their password.
    """
    # Present and process a change password form. User must be logged in,
    # and know their current password.

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    valid_request = None
    reason_bad = None
    successful = False

    oldpassword = ''
    newpassword = ''
    newagain = ''

    # Parse the form data here
    if formdata:
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
            reason_bad = 'You are not currently logged in'
        elif user.validate_password(oldpassword) is False:
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

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    request_valid = None

    username = None
    orcid = None
    email = None

    # Parse the form data here
    thing = None
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
    if thisuser is None or thisuser.superuser is not True:
        return dict(allowed=False)

    template_args = dict(allowed=True)

    # If we got an action, do it
    if username:
        try:
            user = ctx.session.query(User).filter(User.username == username)\
                .one()
            if action == "Grant":
                action_name = 'Granting'
                user.gemini_staff = True
            elif action == "Revoke":
                action_name = 'Revoking'
                user.gemini_staff = False
            else:
                # This shouldn't happen.
                action_name = None
                user = None
            template_args['action_name'] = action_name
            template_args['action_user'] = user
        except NoResultFound:
            template_args['no_result'] = True

    # Have applied changes, now generate list of staff users
    template_args['user_list'] = ctx.session.query(User).\
        order_by(User.gemini_staff, User.username)

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
    if thisuser is None or (thisuser.superuser is not True and
                            thisuser.user_admin is not True):
        return dict(allowed=False)

    template_args = dict(allowed=True)
    template_args['user_list'] = ctx.session.query(User)\
        .order_by(User.gemini_staff, User.username)
    if email and email_inuse(email):
        template_args['email_in_use'] = True
        return template_args
    if email and (('@' not in email) or ('.' not in email) or (',' in email)):
        template_args['email_invalid'] = "Not a valid Email address"
        return template_args

    # If we got an action, do it
    if username:
        try:
            user = ctx.session.query(User).filter(User.username == username)\
                .one()
            user.email = email
            template_args['email_changed'] = True
            template_args['action_user'] = user
        except NoResultFound:
            template_args['no_result'] = True

    # Have applied changes, now generate list of staff users
    template_args['user_list'] = ctx.session.query(User)\
        .order_by(User.gemini_staff, User.username)

    ctx.session.commit()

    return template_args


@templating.templated("user/admin_change_password.html")
def admin_change_password():
    """
    Allows supersusers to set passwords on user accounts
    """

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    username = ''
    password = ''

    # Parse the form data
    if formdata:
        if 'username' in list(formdata.keys()):
            username = formdata['username'].value
        if 'password' in list(formdata.keys()):
            password = formdata['password'].value

    # Permission requires either superuser or user_admin
    thisuser = ctx.user
    if thisuser is None or (thisuser.superuser is not True and
                            thisuser.user_admin is not True):
        return dict(allowed=False)

    template_args = dict(allowed=True)
    template_args['user_list'] = ctx.session.query(User)\
        .order_by(User.gemini_staff, User.username)

    if username:
        try:
            user = ctx.session.query(User).filter(User.username == username)\
                .one()
            user.reset_password(password)
            ctx.session.commit()
            template_args['password_changed'] = True
            template_args['action_user'] = user
        except NoResultFound:
            template_args['no_result'] = True

    # Have applied changes, now generate list of staff users
    template_args['user_list'] = ctx.session.query(User)\
        .order_by(User.gemini_staff, User.username)

    ctx.session.commit()

    return template_args


@templating.templated("user/admin_file_permissions.html")
def admin_file_permissions():
    """
    Allows supersusers to set emails on user accounts
    """

    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    usernames = ''
    item = ''
    filter = ''
    delete = None
    warnings = list()

    # Parse the form data
    if formdata:
        if 'username' in list(formdata.keys()):
            usernames = formdata['username'].value
        if 'item' in list(formdata.keys()):
            item = formdata['item'].value
        if 'filter' in list(formdata.keys()):
            filter = formdata['filter'].value
        if 'delete' in list(formdata.keys()):
            delete = int(formdata['delete'].value)

    # Permission requires either superuser or user_admin
    thisuser = ctx.user
    if thisuser is None or (thisuser.superuser is not True
                            and thisuser.user_admin is not True
                            and thisuser.file_permission_admin is not True):
        return dict(allowed=False)

    template_args = dict(allowed=True)

    if delete:
        ctx.session.query(UserProgram).filter(UserProgram.id == delete).delete()

    # If we got an action, do it
    if item:
        for itemx in item.split(','):
            itemx = itemx.strip()
            if not itemx.endswith('.fits'):
                try:
                    obscheck = GeminiObservation(itemx)
                    if not obscheck.valid:
                        warnings.append(f'Observation ID <b>{itemx}</b> '
                                        'has invalid format, adding anyway')
                except Exception:
                    warnings.append(f'Observation ID <b>{itemx}</b> '
                                    'has invalid format, adding anyway')

    if usernames and item:
        for username in usernames.split(','):
            username = username.strip()
            try:
                user = ctx.session.query(User)\
                    .filter(User.username == username).one()
                for itemx in item.split(','):
                    itemx = itemx.strip()
                    if itemx.endswith('.fits'):
                        up = ctx.session.query(UserProgram)\
                            .filter(UserProgram.filename == itemx) \
                            .filter(UserProgram.user_id == user.id).first()
                        if up is None:
                            up = UserProgram(user_id=user.id, filename=itemx)
                            ctx.session.add(up)
                            ctx.session.flush()
                    else:
                        up = ctx.session.query(UserProgram)\
                            .filter(UserProgram.observation_id == itemx) \
                            .filter(UserProgram.user_id == user.id).first()
                        if up is None:
                            up = UserProgram(user_id=user.id,
                                             observation_id=itemx)
                            ctx.session.add(up)
                            ctx.session.flush()
            except NoResultFound:
                warnings.append(f'Username <b>{username}</b> not found in '
                                'system, ignoring')

    observation_list = list()
    q = ctx.session.query(UserProgram, User)\
        .filter(and_(UserProgram.observation_id != None,
                     UserProgram.observation_id != ''),
                User.id == UserProgram.user_id)
    if filter:
        q = q.filter(or_(UserProgram.observation_id == filter,
                         User.username == filter))
    for up, usr in q.order_by(UserProgram.observation_id.desc(), User.username):
        obs_perm = dict()
        obs_perm['id'] = up.id
        obs_perm['username'] = usr.username
        obs_perm['observation_id'] = up.observation_id
        observation_list.append(obs_perm)

    user_list = list()
    q = ctx.session.query(User).order_by(User.username)
    for u in q.all():
        usr = dict()
        usr['username'] = u.username
        usr['fullname'] = u.fullname
        usr['email'] = u.email
        user_list.append(usr)

    file_list = list()
    q = ctx.session.query(UserProgram, User)\
        .filter(and_(UserProgram.filename != None,
                     UserProgram.filename != ''),
                     User.id == UserProgram.user_id)
    if filter:
        q = q.filter(or_(UserProgram.filename == filter,
                         User.username == filter))
    for up, usr in q.order_by(UserProgram.filename.desc(), User.username):
        obs_perm = dict()
        obs_perm['id'] = up.id
        obs_perm['username'] = usr.username
        obs_perm['filename'] = up.filename
        file_list.append(obs_perm)

    # Have applied changes, now generate list of staff users
    template_args['observation_list'] = observation_list
    template_args['user_list'] = user_list
    template_args['file_list'] = file_list
    template_args['filter'] = filter
    template_args['warnings'] = warnings

    ctx.session.commit()

    return template_args


@templating.templated("user/login.html")
def login(things):
    """
    Presents and processes a login form
    Sends session cookie if successful
    """
    ctx = get_context()

    # Process the form data first if there is any
    formdata = ctx.get_form_data()
    valid_request = None
    reason_bad = None

    username = ''
    password = ''
    redirect = ''
    cookie = None

    # Parse the form data here
    if formdata:
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
            user = ctx.session.query(User)\
                .filter(User.username == username).one()
            if user.validate_password(password):
                # Successful login
                cookie = user.log_in(by='local_account')
                valid_request = True
            else:
                reason_bad = 'Username / password not valid. ' \
                             'If you need to reset your password, ' \
                             '<a href="/request_password_reset">Click Here</a>'

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
            ctx.log("Logout - Multiple Users with same session cookie: %s"
                    % cookie)
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
    Tells you who you are logged in as, and presents the account maintenance
    links
    """
    # Find out who we are if logged in
    fsc = get_config()
    template_args = {'orcid_enabled': fsc.orcid_enabled}

    user = get_context().user

    try:
        template_args['username'] = user.username
        template_args['orcid_id'] = user.orcid_id
        template_args['noirlab_id'] = user.noirlab_id
        template_args['preferred_id'] = user.orcid_id or user.noirlab_id or \
                                        user.username
        template_args['fullname'] = user.fullname
        template_args['is_superuser'] = user.superuser
        template_args['user_admin'] = user.user_admin
        template_args['file_permission_admin'] = user.file_permission_admin
    except AttributeError:
        # no user
        pass

    # Construct the "things" part of the URL for the link that want to be
    # able to take you back to the same form contents
    template_args['thing_string'] = '/'.join(things)

    return template_args


@templating.templated("user/list.html")
def user_list():
    """
    Displays a list of archive users. Must be logged in as a gemini_staff
    user to see this.
    """

    ctx = get_context()

    thisuser = ctx.user
    if thisuser is None or thisuser.gemini_staff is not True:
        return dict(staffer=False)

    users = (ctx.session.query(User)
             .order_by(desc(User.superuser),
                       desc(User.gemini_staff),
                       User.username))

    return dict(staff=True,
                users=users)


def email_inuse(email):
    """
    Check the database to see if this email is already in use. Returns True
    if it is, False otherwise.

    Email addresses are case-insensitive, so we have to get clever here to do
    a case-insensitive match.  Also, in future we can add a constraint to the
    database.  However, currently we already have existing users with
    duplicate emails.  So, until we have a strategy for how to unwind those
    accounts we can't constrain the database.  A trigger would be another
    option, but we'd still want to catch it here to alert the user.  I'm
    inclined to just fix the account data and then add a proper unique
    constraint and skip doing any kind of trigger validation.
    """
    num = get_context().session.query(User).\
        filter(User.email.ilike(email)).count()

    return num != 0


def username_inuse(username):
    """
    Check the database to see if a username is already in use. Returns True
    if it is, False otherwise
    """

    num = get_context().session.query(User).\
        filter(User.username == username).count()
    return num != 0


def orcid_inuse(orcid):
    """
    Check the database to see if a username is already in use. Returns True
    if it is, False otherwise
    """

    num = get_context().session.query(User).\
        filter(User.orcid_id == orcid).count()
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


def needs_cookie(magic_cookie, content_type='text/html', annotate=None,
                 context=None, fsconfig=None):
    """
    Decorator for functions that need a magic cookie value to be passed with the
    request. The basic use is (notice the decorator parenthesis, they're
    important)::

       @needs_cookie()
       def decorated_function():
           ...

    Which rejects access if no valid magic cookie value was supplied with the
    request.

    If the magic cookie name is None, we reject the request irrespective
    of the supplied value (ie setting the cookie name to None disables
    access using cookies)

    magic cookie is the cookie name. The decorator function will look up the
    acceptable value(s) from a config system parameter with the same name. This
    will be a list of acceptable values. If this value is None or [None],
    access using cookies is disabled.

    See also: needs_login()
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kw):

            # This is to allow passing in a fake context object for testing
            ctx = context if context is not None else get_context()

            ctype = 'application/json' if content_type == 'json' \
                else 'text/html'

            # This to allow passing in a fake config object for testing
            fsc = fsconfig if fsconfig is not None else get_config()

            # Is this a development server with authentication bypassed?
            # fsc is passable as an argument to facilitate testing
            if fsc.fits_system_status == 'development' \
                    and fsc.development_bypass_auth:
                return fn(*args, **kw)

            # Did we get a valid magic cookie name?
            got_magic = False
            if magic_cookie is not None:
                magic_values = fsc.get(magic_cookie)
                if magic_values:
                    for magic_value in magic_values:
                        if magic_value is None:
                            continue
                        try:
                            if cookie_match(ctx.cookies[magic_cookie],
                                            magic_value):
                                got_magic = True
                                break
                        except KeyError:
                            pass

            if not got_magic:
                raise_error = functools.partial(ctx.resp.client_error,
                                                code=Return.HTTP_FORBIDDEN,
                                                content_type=ctype,
                                                annotate=annotate)
                raise_error(message="This resource can only be accessed by "
                                    "providing a valid magic cookie, "
                                    "which this request did not.")

            return fn(*args, **kw)

        return wrapper

    return decorator


# Helper function - return True if the value matches the reference value,
# or if the reference value is a json list, if the value matches any value
# in the list - makes value migration easier
def cookie_match(value, reference):
    if value == reference:
        return True
    try:
        references = json.loads(reference)
        if isinstance(references, list):
            for ref in references:
                if value == ref:
                    return True
    except Exception:
        # was not a json list
        pass
    return False


def needs_login(staff=False, misc_upload=False, superuser=False,
                content_type='text/html', annotate=None,
                context=None, fsconfig=None):
    """
    Decorator for functions that need a user to be logged in.
    The basic use is (notice the decorator parenthesis, they're important)::

           @needs_login()
           def decorated_function():
               ...

    Which rejects access if the user is not logged in. The decorator accepts
    a number of keyword arguments. The most restrictive is the one that
    applies:

    ``staff=(False|True)``
         If ``True``, the user must be staff

    ``superuser=(False|True)``
         If ``True``, the user must be a superuser

    ``content_type='...'``
         Can be set to ``'html'`` (the default) or ``'json'``, depending on
         the kind of answer we want to provide, in case that the access is
         forbidden
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kw):

            # This is used to inject a fake context for testing
            ctx = context if context is not None else get_context()

            ctype = 'application/json' if content_type == 'json' \
                else 'text/html'

            # Is this a development server with authentication bypassed?
            # fsc is passable as an argument to facilitate testing
            fsc = fsconfig if fsconfig is not None else get_config()
            if fsc.fits_system_status == 'development' \
                    and fsc.development_bypass_auth:
                return fn(*args, **kw)

            user = ctx.user

            raise_error = functools.partial(ctx.resp.client_error,
                                            code=Return.HTTP_FORBIDDEN,
                                            content_type=ctype,
                                            annotate=annotate)
            if not user:
                raise_error(message="You need to be logged in to access "
                                    "this resource")
            if superuser is True and not user.superuser:
                raise_error(message="You need to be logged in as a "
                                    "superuser to access this resource")
            if staff is True and not user.gemini_staff:
                raise_error(message="You need to be logged in as Gemini "
                                    "Staff member to access this resource")
            if misc_upload is True and not user.misc_upload:
                raise_error(message="You need to be logged in with misc "
                                    "upload permission to access this resource")
            return fn(*args, **kw)

        return wrapper

    return decorator


@templating.templated("user/oauth.html")
def oauth(service, code):
    """
    Generates and handles OAuth (e.g. ORCID, NOIRlab SSO) backed accounts

    ``service`` is the OAuth service to use. Either 'ORCID' or 'NOIRlab'.

    ``code`` This is the Oauth supplied authentication code.  We can use this
    to request the users identity from the OAuth service.  On the first pass
    to this endpoint there is no code, and we redirect the user to the respective
    Oauth login page.
    """

    fsc = get_config()
    if not fsc.oauth_enabled:
        return dict(
            notification_message="",
            reason_bad="OAuth not enabled on this system"
        )

    if service == 'NOIRlab':
        oauth_server = fsc.noirlab_oauth_server
        client_id = fsc.noirlab_oauth_client_id
        client_secret = fsc.noirlab_oauth_client_secret
        redirect_url = fsc.noirlab_oauth_redirect_url
        response_id_key = 'username'
        user_id_key = 'noirlab_id'
    elif service == 'ORCID':
        oauth_server = ''
        client_id = ''
        client_secret = ''
        redirect_url = ''
        response_id_key = 'orcid'
        user_id_key = 'orcid_id'
    else:
        return dict(
            notification_message="",
            reason_bad=f"OAuth for {service} not configured on this system"
        )

    notification_message = ""
    reason_bad = ""

    ctx = get_context()

    if code:
        # User came back from OAuth service with a code.
        # Need to POST the code back to the OAuth service to get the credentials
        # And we need to do this with HTTP Basic Auth
        basic = HTTPBasicAuth(client_id, client_secret)
        data = {
            "client_id": client_id,
            # "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_url
        }
        oauth_token_url = f'https://{oauth_server}/token'
        # Note, we don't post JSON here, it's an
        # application/x-www-form-urlencoded POST.
        r = requests.post(oauth_token_url, data=data, auth=basic)
        print(f'POST Request headers: {r.request.headers}')
        print(f'POST Request body: {r.request.body}')
        print(f'POST Headers: {r.headers}')
        print(f'POST Response text: {r.text}')
        if r.status_code == 200:
            response_data = r.json()
            id_token = response_data.get('id_token')
            if id_token is None:
                return dict(
                    notification_message="",
                    reason_bad=f"Did not get a valid id token token from"
                               f" oauth service {service}"
                )
            decoded_id = jwt.decode(id_token,
                                    options={"verify_signature": False})
            oauth_id = decoded_id['sub']

            # Find any existing user entry for this oauth_id.
            user = ctx.session.query(User)
            user = user.filter(getattr(User, user_id_key) == oauth_id)
            user = user.one_or_none()

            if user is None:
                # Authenticated via OAuth, but we haven't seen this oauth_id
                # (orcid_id or noirlab_id) before
                if ctx.user:
                    # But we have a valid session cookie for an existing user
                    # So associate this OAuth ID with that user
                    setattr(ctx.user, user_id_key, oauth_id)
                    #ctx.session.save(ctx.user)
                    ctx.session.commit()
                else:
                    # No valid session cookie, but maybe an existing user
                    # Do we recognize their oauth email address?
                    user = ctx.session.query(User)\
                        .filter(User.email == decoded_id['email'])\
                        .one_or_none()
                    if user:
                        # OAuth Email matches a user email. Add this OAuth ID
                        # to that user
                        setattr(ctx.user, user_id_key, oauth_id)
                        ctx.session.commit()
                    else:
                        # We don't recognize them at all. Create new user for
                        # them and associate this oauth_id
                        user = User('')
                        setattr(user, user_id_key, oauth_id)
                        user.fullname = f"{decoded_id['firstname']} " \
                                        f"{decoded_id['lastname']}"
                        user.email = decoded_id['email']
                        ctx.session.add(user)
                        ctx.session.commit()

            cookie = user.log_in(by=service)
            exp = datetime.datetime.utcnow() + datetime.timedelta(days=365)
            ctx.cookies.set('gemini_archive_session', cookie,
                            expires=exp, path="/")
            ctx.resp.redirect_to('/searchform')
        else:
            reason_bad = "Error communicating with OAuth service " \
                         f"- status code {r.status_code}"
    else:
        # No auth code - Send them to the OAuth server to authenticate, which
        # will send them back here with their code
        oauth_url = f'https://{oauth_server}/authorize?client_id=' \
                    f'{client_id}' \
                    f'&response_type=code&scope=openid' \
                    f'&redirect_uri={urllib.parse.quote(redirect_url)}'
        ctx.resp.redirect_to(oauth_url)

    template_args = dict(
        notification_message=notification_message,
        reason_bad=reason_bad,
        via='OAuth'
    )

    return template_args
