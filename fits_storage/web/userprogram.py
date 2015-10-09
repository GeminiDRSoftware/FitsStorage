"""
This module handles the web interface to the userprogram orm.
This is how users register programs against their userids,
find out what programs they have access to etc
"""

from ..orm import sessionfactory

from ..orm.userprogram import UserProgram

from .user import userfromcookie
from . import templating

# This will only work with apache
from mod_python import apache
from mod_python import util

import urllib

@templating.templated("user_programs.html", with_session=True)
def my_programs(session, req, things):
    """
    Generates a page showing the user what programs
    they have registered access to.
    Also generates and processes a form to add new ones.
    """

    # First, process the form data if there is any
    formdata = util.FieldStorage(req)

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

    user = userfromcookie(session, req)
    if user:
        username = user.username
        if program_id or program_key:
            reason_bad = request_user_program(session, user, program_id, program_key)
        prog_list = get_program_list(session, user)

    if username == '':
        return dict(logged_in = False)

    template_args = dict(
        # Build the thing_string to link back to the searchform
        logged_in    = True,
        username     = username,
        progs        = prog_list,
        thing_string = '/'.join(things),
        reason_bad   = reason_bad
        )

    return template_args

def get_program_list(session, user):
    """
    Given a database session and a user object, return
    a list of program IDs that the user has registered.
    """

    prog_list = []
    if user is not None:
        query = session.query(UserProgram).filter(UserProgram.user_id == user.id)
        results = query.all()
        for result in results:
            prog_list.append(result.program_id)

    return prog_list

def request_user_program(session, user, program_id, program_key):
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

    # Is this program ID already registered for this user?
    query = session.query(UserProgram).filter(UserProgram.user_id == user.id).filter(UserProgram.program_id == program_id)
    if query.count() > 0:
        return "That program ID is already registered for this user"

    valid = validate_program_key(program_id, program_key)

    if valid:
        userprog = UserProgram(user.id, program_id)
        session.add(userprog)
        session.commit()
        return ""
    else:
        return "Key not valid for program"

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

    ufd = urllib.urlopen(url)
    reply = ufd.read()
    ufd.close()

    if reply[:3] == 'YES':
        return True
    else:
        return False
