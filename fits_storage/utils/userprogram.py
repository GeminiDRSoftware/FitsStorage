"""
This module contains utility functions for user matters such as access control
"""

import datetime
from mod_python import Cookie

from sqlalchemy import func

from ..gemini_metadata_utils import ONEDAY_OFFSET

from ..web.userprogram import get_program_list

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.obslog import Obslog
from ..orm.miscfile import MiscFile

from .web import get_context

def canhave_coords(session, user, header, gotmagic=False, user_progid_list=None):
    """
    Returns a boolean saying whether or not the given user can have
    access to the given header coordinates.
    You can optionally pass in the users program list directly. If you
    don't, then this function will look it up, which requires the session
    to be valid. If you pass in the user program list, you don't actually
    need to pass a valid session - it can be None.
    """

    # Is not even proprietary coordinate data
    if not header.proprietary_coordinates:
        return True

    # If it is proprietary coordinate data
    # the rules are exactly the same as for the data
    return canhave_header(session, user, header, gotmagic=gotmagic, user_progid_list=user_progid_list)

def is_staffer(user, filedownloadlog):
    """
    Is the current user a staff member?
    """
    # Is the user gemini staff?
    if user is not None and user.gemini_staff is True:
        if filedownloadlog:
            filedownloadlog.staff_access = True
        return True
    return False

def is_released(release_date, filedownloadlog):
    # Can't compare a datetime to a date
    if isinstance(release_date, datetime.datetime):
        release_date = release_date.date()

    # Is the release date in the past?
    today = datetime.datetime.utcnow().date()
    if release_date and today >= release_date:
        if filedownloadlog:
            filedownloadlog.released = True
        return True
    return False

def reset_pi_access(filedownloadlog):
    if filedownloadlog:
        filedownloadlog.released = False
        filedownloadlog.staff_access = False
        filedownloadlog.eng_access = False
        filedownloadlog.magic_access = False
        filedownloadlog.pi_access = False

def is_users_program(session, user, user_progid_list, program_id, filedownloadlog):
    # If we didn't get passed in the users program list, get it
    if user_progid_list is None:
        user_progid_list = get_program_list(user)

    # Is the program in the list?
    if program_id in user_progid_list:
        if filedownloadlog:
            filedownloadlog.pi_access = True
        return True
    return False

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

    def is_eng():
        # Is the data engineering?
        if header.engineering is True:
            if filedownloadlog:
                filedownloadlog.eng_access = True
            return True
        return False

    clear = any([is_eng(),
                 is_staffer(user, filedownloadlog),
                 is_released(header.release, filedownloadlog)])

    if clear:
        return True

    # If none of the above, then user is requesting pi access
    reset_pi_access(filedownloadlog)

    # Last chance. If not the PI, then deny access
    return is_users_program(session, user, user_progid_list, header.program_id, filedownloadlog)

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

    clear = any([is_staffer(user, filedownloadlog),
                 is_users_program(session, user, user_progid_list, obslog.program_id, filedownloadlog)])

    if clear:
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
        return is_released(maxrel, filedownloadlog)
    else:
        # If we didn't get a release date, assume it's public (eg eng data?)
        return True

    # If none of the above, then deny access
    return False

def canhave_miscfile(session, user, misc, filedownloadlog=None, gotmagic=False, user_progid_list=None):
    """
    Returns a boolean saying whether or not the given user can have
    access to the given miscellaneous file.

    You can optionally pass in the users program list directly. If you
    don't, then this function will look it up, which requires the session
    to be valid. If you pass in the user program list, you don't actually
    need to pass a valid session - it can be None.

    If you pass in a FileDownloadLog object, we will update it to note
    the file access rules that were used.
    """

    clear = any([is_staffer(user, filedownloadlog),
                 is_released(misc.release, filedownloadlog)])

    if clear:
        return True

    # If none of the above, then user is requesting pi access
    reset_pi_access(filedownloadlog)

    # Last chance. If not the PI, then deny access
    return is_users_program(session, user, user_progid_list, misc.program_id, filedownloadlog)

def cant_have(*args, **kw):
    return False

# IMPORTANT: The following comments are used by the autodoc functionality when
#            generating the reference doc. Please, keep them updated
#: ::
#:
#:   orm.header.Header     -> canhave_header
#:   orm.obslog.Obslog     -> canhave_obslog
#:   orm.miscfile.MiscFile -> canhave_miscfile
icanhave_function = {
    Header:   canhave_header,
    Obslog:   canhave_obslog,
    MiscFile: canhave_miscfile,
}

def icanhave(ctx, item, filedownloadlog=None):
    """
    Convenience function to determine if a user has rights to a certain piece of data.
    The user information is obtained from the ``req`` object.

    Returns a boolean saying whether the requesting client can have
    access to the given item. The item has to be an instance of one of the
    ``orm`` classes indexed in ``icanhave_function``

    If a ``FileDownloadLog`` instance is passed, it will be updated to note access to
    the relevant content.
    """

    # Does the client have the magic cookie?
    gotmagic = ctx.got_magic
    if gotmagic:
        if filedownloadlog:
            filedownloadlog.magic_access = True
        return True

    fn = icanhave_function.get(item.__class__, cant_have)
    return fn(ctx.session, ctx.user, item, filedownloadlog, gotmagic=gotmagic)
