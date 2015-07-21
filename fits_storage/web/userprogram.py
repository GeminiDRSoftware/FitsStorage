"""
This module handles the web interface to the userprogram orm.
This is how users register programs against their userids,
find out what programs they have access to etc
"""

from orm import sessionfactory

from orm.userprogram import UserProgram

from web.user import userfromcookie

# This will only work with apache
from mod_python import apache
from mod_python import util

import urllib

def my_programs(req, things):
    """
    Generates a page showing the user what programs
    they have registered access to.
    Also generates and processes a form to add new ones.
    """

    # Build the thing_string to link back to the searchform
    thing_string = ''
    if things:
        thing_string = '/' + '/'.join(things)

    # First, process the form data if there is any
    formdata = util.FieldStorage(req)

    program_id = ''
    program_key = ''
    if len(formdata.keys()) > 0:
        if 'program_id' in formdata.keys():
            program_id = formdata['program_id'].value
        if 'program_key' in formdata.keys():
            program_key = formdata['program_key'].value

    # Now figure out if we are logged in, who we are, and current prog_list
    # If we have form data, try and action it
    username = ''
    prog_list = []
    reason_bad = ''

    try:
        session = sessionfactory()
        user = userfromcookie(session, req)
        if user:
            username = user.username
            if program_id or program_key:
                reason_bad = request_user_program(session, user, program_id, program_key)
            prog_list = get_program_list(session, user)
    finally:
        session.close()


    req.content_type = "text/html"
    req.write("<html><head><title>Gemini Archive Registered Program List</title></head><body>")
    if username == '':
        req.write('<h1>Not logged in</h1>')
        req.write('<p>You need to be logged in to see your registered programs.</p>')
        req.write('<p>You can <a href="/login">log in here</a></p>')
        req.write('</body></html>')
        return apache.HTTP_OK

    if len(prog_list) == 0:
        req.write('<h1>No programs registered</h1>')
        req.write('<p>There are currently no programs registered to username: %s</p>' % username)
    else:
        req.write('<h1>Registered Program list for %s</h1>' % username)
        req.write('<ul>')
        for prog in prog_list:
            req.write('<li>%s</li>' % prog)
        req.write('</ul>')

    if reason_bad:
        req.write("<h2>Registering your new program failed</h2>")
        req.write('<p>%s</p>' % reason_bad)

    req.write('<p><a href="/searchform%s">Click here to return to your search form</a></p>' % thing_string)

    req.write('<h2>Register a new program</h2>')
    req.write('<p>To register a new program with your account, fill out and submit the form below</p>')

    req.write('<FORM action="/my_programs%s" method="POST">' % thing_string)
    req.write('<TABLE>')
    req.write('<TR><TD><LABEL for="program_id">Program ID</LABEL><TD>')
    req.write('<TD><INPUT type="text" size=16 name="program_id"></INPUT></TD></TR>')
    req.write('<TR><TD><LABEL for="program_key">Program Key</LABEL><TD>')
    req.write('<TD><INPUT type="text" size=8 name="program_key"></INPUT></TD></TR>')
    req.write('</TABLE>')
    req.write('<INPUT type="submit" value="Submit"></INPUT>')
    req.write('</FORM>')
    req.write("</body></html>")
    return apache.HTTP_OK


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
