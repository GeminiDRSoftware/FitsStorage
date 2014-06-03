"""
This module contains utility functions for user matters such as access control
"""

import datetime
from mod_python import Cookie

from fits_storage_config import magic_download_cookie

from web.userprogram import get_program_list
from web.user import userfromcookie

def icanhave(session, req, header):
    """
    Returns a boolean saying whether the requesting client can have
    access to the given header
    """

    user=userfromcookie(session, req)
    return canhave(session, req, user, header)


def canhave(session, req, user, header):
    """
    Returns a boolean saying whether or not the given user can have
    access to the given header
    """

    # All calibration data are immediately public
    # Question - does this include progCal?
    if(header.observation_class in ['dayCal', 'partnerCal', 'acqCal', 'progCal']):
        return True

    # Is the release date in the past?
    today = datetime.datetime.utcnow().date()
    if((header.release) and (today >= header.release)):
        return True

    # Is the user gemini staff?
    if(user.gemini_staff is True):
        return True

    # Is the data engineering?
    if (header.engineering is True):
        return True

    # Does the client have the magic cookie?
    if (got_magic(req)):
        return True

    # If none of the above, then it's proprietary data
    progid_list = get_program_list(session, user)
    if(header.program_id in progid_list):
        return True

    # If none of the above, then deny access
    return False


def got_magic(req):
    """
    Returns a boolean to say whether or not the client has
    the magic authorization cookie
    """

    if(magic_download_cookie is None):
        return False

    cookies = Cookie.get_cookies(req)
    got = False
    if(cookies.has_key('gemini_fits_authorization')):
        auth = cookies['gemini_fits_authorization'].value
        if(auth == magic_download_cookie):
            got = True

    return got
