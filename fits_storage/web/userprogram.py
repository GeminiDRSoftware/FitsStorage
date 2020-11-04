"""
This module handles the web interface to the userprogram orm.
This is how users register programs against their userids,
find out what programs they have access to etc
"""

from ..orm.userprogram import UserProgram
from ..utils.web import get_context

from . import templating

import urllib.request, urllib.parse, urllib.error

@templating.templated("user_programs.html")
def my_programs(things):
    """
    Generates a page showing the user what programs
    they have registered access to.
    Also generates and processes a form to add new ones.
    """

    ctx = get_context()

    # First, process the form data if there is any
    formdata = ctx.get_form_data()

    program_id = ''
    program_key = ''
    if formdata:
        if 'program_id' in formdata:
            program_id = formdata['program_id'].value.strip()
        if 'program_key' in formdata:
            program_key = formdata['program_key'].value.strip()

    # Now figure out if we are logged in, who we are, and current prog_list
    # If we have form data, try and action it
    username = ''
    prog_list = []
    reason_bad = ''
    orcid = ''

    user = ctx.user
    if user:
        username = user.username
        orcid = user.orcid_id
        if program_id or program_key:
            reason_bad = request_user_program(user, program_id, program_key)
        prog_list = get_program_list(user)

    if username == '' and not orcid:
        return dict(logged_in = False)

    someid = username
    if not someid:
        someid = orcid
    template_args = dict(
        # Build the thing_string to link back to the searchform
        logged_in    = True,
        username     = someid,
        progs        = prog_list,
        thing_string = '/'.join(things),
        reason_bad   = reason_bad
        )

    return template_args


def get_permissions_list(user):
    """
    Find all three types of permissions for a user and return them
    in one go
    """

    prog_list = []
    obsid_list = []
    file_list = []

    if user is not None:
        query = get_context().session.query(UserProgram).filter(UserProgram.user_id == user.id)
        results = query.all()
        for result in results:
            if result.program_id:
                prog_list.append(result.program_id)
            if result.observation_id:
                obsid_list.append(result.observation_id)
            if result.filename:
                file_list.append((result.path if result.path else "", result.filename))

    return prog_list, obsid_list, file_list


def get_program_list(user):
    """
    Given a database session and a user object, return
    a list of program IDs that the user has registered.
    """

    prog_list = []
    if user is not None:
        query = get_context().session.query(UserProgram).filter(UserProgram.user_id == user.id)
        results = query.all()
        for result in results:
            if result.program_id:
                prog_list.append(result.program_id)

    return prog_list


def get_obsid_list(user):
    """
    Given a database session and a user object, return
    a list of observation IDs that the user has permission for.
    """

    obsid_list = []
    if user is not None:
        query = get_context().session.query(UserProgram).filter(UserProgram.user_id == user.id)
        results = query.all()
        for result in results:
            if result.observation_id:
                obsid_list.append(result.observation_id)

    return obsid_list


def get_file_list(user):
    """
    Given a database session and a user object, return
    a list of observation IDs that the user has permission for.

    Returns results as a list of tuples (path, filename)
    """

    file_list = []
    if user is not None:
        query = get_context().session.query(UserProgram).filter(UserProgram.user_id == user.id)
        results = query.all()
        for result in results:
            if result.filename:
                file_list.append((result.path, result.filename))

    return file_list


def request_user_program(user, program_id, program_key):
    """
    Requests to register a program_id for a user
    Returns an empty string if sucessfull, a reason why not if not
    This function does commit the changes to the database
    """

    if user is None:
        return "Invalid user"
    if len(program_id) < 8:
        return "Invalid program ID"
    if len(program_key) < 5:
        return "Invalid program key"

    session = get_context().session

    # Is this program ID already registered for this user?
    query = session.query(UserProgram).filter(UserProgram.user_id == user.id).filter(UserProgram.program_id == program_id)
    if query.count() > 0:
        return "That program ID is already registered for this user"

    try:
        valid = validate_program_key(program_id, program_key)

        if valid:
            userprog = UserProgram(user.id, program_id)
            session.add(userprog)
            session.commit()
            return ""
        else:
            return "Key not valid for program"
    except Exception:
        return "Unable to verify program key with ODB"


def validate_program_key(program_id, program_key):
    """
    Query the ODB to see if this is a valid program ID / program key
    combination. Return True if valid, False otherwise
    """

    if program_id[:2] == 'GN':
        host = 'gnodb'
    elif program_id[:2] == 'GS':
        host = 'gsodb'
    else:
        return False

    url = 'https://%s.gemini.edu:8443/auth?id=%s&password=%s' % (host, program_id, program_key)

    # REMOVE THIS ONCE WE GET SSL WORKING, THEY ARE SELF SIGNED
    import os, ssl
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    ufd = urllib.request.urlopen(url)
    reply = ufd.read().decode('utf-8')
    ufd.close()

    if reply[:3] == 'YES':
        return True
    else:
        return False
