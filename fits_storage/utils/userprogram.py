"""
This module contains utility functions for user matters such as access control
"""

import datetime
from mod_python import Cookie

from sqlalchemy import func

from ..fits_storage_config import magic_download_cookie
from ..gemini_metadata_utils import ONEDAY_OFFSET

from ..web.userprogram import get_program_list
from ..web.user import userfromcookie

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.obslog import Obslog

def icanhave(session, req, item, filedownloadlog=None):
    """
    Returns a boolean saying whether the requesting client can have
    access to the given item. The item is either a header object or an obslog object
    """

    # Does the client have the magic cookie?
    gotmagic = got_magic(req)
    if gotmagic:
        if filedownloadlog:
            filedownloadlog.magic_access = True
        return True

    # Get the user from the request
    user = userfromcookie(session, req)

    if isinstance(item, Header):
        return canhave_header(session, user, item, filedownloadlog, gotmagic=gotmagic)

    if isinstance(item, Obslog):
        return canhave_obslog(session, user, item, filedownloadlog, gotmagic=gotmagic)


def canhave_header(session, user, header, filedownloadlog=None, gotmagic=False, user_progid_list=None):
    """
    Returns a boolean saying whether or not the given user can have
    access to the given header.
    You can optionally pass in the users program list directly. If you
    don't, then this function will look it up, which requires the session
    to be valid. If you pass in the user program list, you don't actually
    need to pass a valid session - it can be None.
    If you pass in a FileDownloadLog object, we will update it to note
    the file access rules that were used.
    """

    # Is the user gemini staff?
    if user is not None and user.gemini_staff is True:
        if filedownloadlog:
            filedownloadlog.staff_access = True
        return True

    # Is the release date in the past?
    today = datetime.datetime.utcnow().date()
    if header.release and today >= header.release:
        if filedownloadlog:
            filedownloadlog.released = True
        return True

    # Is the data engineering?
    if header.engineering is True:
        if filedownloadlog:
            filedownloadlog.eng_access = True
        return True

    # If none of the above, then user is requesting pi access
    if filedownloadlog:
        filedownloadlog.released = False
        filedownloadlog.staff_access = False
        filedownloadlog.eng_access = False
        filedownloadlog.magic_access = False
        filedownloadlog.pi_access = False

    # If we didn't get passed in the users program list, get it
    if user_progid_list is None:
        user_progid_list = get_program_list(session, user)

    # Is the program in the list?
    if header.program_id in user_progid_list:
        if filedownloadlog:
            filedownloadlog.pi_access = True
        return True

    # If none of the above, then deny access
    return False

def canhave_obslog(session, user, obslog, filedownloadlog=None, gotmagic=False, user_progid_list=None):
    """
    Returns a boolean saying whether or not the given user can have
    access to the given obslog.
    You can optionally pass in the users program list directly. If you
    don't, then this function will look it up, which requires the session
    to be valid. If you pass in the user program list, you don't actually
    need to pass a valid session - it can be None.
    If you pass in a FileDownloadLog object, we will update it to note
    the file access rules that were used.
    """

    # Is the user gemini staff?
    if user is not None and user.gemini_staff is True:
        if filedownloadlog:
            filedownloadlog.staff_access = True
        return True

    # Is this a PI requesting their obslogs

    # If we didn't get passed in the users program list, get it
    if user_progid_list is None:
        user_progid_list = get_program_list(session, user)

    # Is the program in the list?
    if obslog.program_id in user_progid_list:
        if filedownloadlog:
            filedownloadlog.pi_access = True
        return True

    # As agreed by PH, AA, IJ, BM:
    # An obslog references data from a given program on a given night -
    # The obslog goes public when all the data for that program on that night are public
    # In this case, that has to be release date as PI access etc is taken care of above
    # Find the maximum release date for the affected data
    maxrel_query = session.query(func.max(Header.release)).select_from(Header, DiskFile)
    maxrel_query = maxrel_query.filter(Header.diskfile_id == DiskFile.id)
    maxrel_query = maxrel_query.filter(DiskFile.canonical == True)
    maxrel_query = maxrel_query.filter(Header.program_id == obslog.program_id)

    zerohour = datetime.time(0, 0, 0)
    start = datetime.datetime.combine(obslog.date, zerohour)
    end = start + ONEDAY_OFFSET
    maxrel_query = maxrel_query.filter(Header.ut_datetime >= start).filter(Header.ut_datetime < end)

    maxrel = maxrel_query.first()[0]

    # did we get a release date (ie does any data exist and have release dates?)
    if maxrel:
        # Is the release date in the past?
        maxrel = datetime.datetime.combine(maxrel, zerohour)
        if maxrel <= datetime.datetime.utcnow():
            return True
        else:
            return False
    else:
        # If we didn't get a release date, assume it's public (eg eng data?)
        return True

    # If none of the above, then deny access
    return False

def got_magic(req):
    """
    Returns a boolean to say whether or not the client has
    the magic authorization cookie
    """

    if magic_download_cookie is None:
        return False

    cookies = Cookie.get_cookies(req)
    got = False
    if cookies.has_key('gemini_fits_authorization'):
        auth = cookies['gemini_fits_authorization'].value
        if auth == magic_download_cookie:
            got = True

    return got
