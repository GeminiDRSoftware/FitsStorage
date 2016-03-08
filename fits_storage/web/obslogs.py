"""
This module contains the obslogs web summary code.
"""
import datetime

from ..orm import NoResultFound
from ..orm.obslog import Obslog
from ..fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from .list_headers import list_obslogs, list_headers
from .selection import sayselection, openquery
from ..apache_return_codes import HTTP_OK

from ..utils.userprogram import icanhave
from ..utils.web import Context

from ..orm.querylog import QueryLog

from . import templating

def add_summary_completed():
    ctx = Context()
    try:
        querylog = ctx.session.query(QueryLog).filter(QueryLog.usagelog_id == ctx.usagelog.id).one()
        querylog.summary_completed = datetime.datetime.utcnow()
    except NoResultFound:
        # Shouldn't happen, but just in case...
        pass

def generate_obslogs(req, obslogs):
    session = Context().session
    for obslog in obslogs:
        yield obslog.diskfile.file.name, obslog.date, obslog.program_id, icanhave(session, req, obslog)

@templating.templated("obslog/obslogs.html", at_end_hook=add_summary_completed)
def obslogs(req, selection, sumtype):
    """
    This is the obslogs summary generator
    """

    # Instantiate querylog, populate initial fields
    querylog = QueryLog(Context().usagelog)
    querylog.summarytype = sumtype
    querylog.selection = str(selection)
    querylog.query_started = datetime.datetime.utcnow()

    # If this is associated_obslogs, we do the association here
    if sumtype == 'associated_obslogs':
        querylog.add_note("Associated Obslogs")
        headers = list_headers(selection, None)
        obslogs = associate_obslogs(headers)
    else:
        # Simple obslog search
        obslogs = list_obslogs(selection, None)

    querylog.query_completed = datetime.datetime.utcnow()
    num_results = len(obslogs)
    querylog.numresults = num_results

    # Did we get any selection warnings?
    if 'warning' in selection:
        querylog.add_note("Selection Warning: %s" % selection['warning'])
    # Note any notrecognised in the querylog
    if 'notrecognised' in selection:
        querylog.add_note("Selection NotRecognised: %s" % selection['notrecognised'])
    # Note in the log if we hit limits
    if num_results == fits_open_result_limit:
        querylog.add_note("Hit Open search result limit")
    if num_results == fits_closed_result_limit:
        querylog.add_note("Hit Closed search result limit")

    session = Context().session
    session.add(querylog)
    session.flush()

    return dict(
        hits_open    = openquery(selection) and (len(obslogs) == fits_open_result_limit),
        hits_closed  = len(obslogs) == fits_closed_result_limit,
        open_limit   = fits_open_result_limit,
        closed_limit = fits_closed_result_limit,
        selection    = selection,
        no_obslogs   = num_results == 0,
        obslogs      = generate_obslogs(req, obslogs)
        )

def associate_obslogs(headers):
    """
    Generate a list of obslogs that are associated with the given headers
    """

    obslogs = []

    # Could do this more efficiently by grouping the header query by date and progid, but this will do for now
    session = Context().session
    for header in headers:
        query = session.query(Obslog).filter(Obslog.date == header.ut_datetime.date()).filter(Obslog.program_id == header.program_id)
        for result in query:
            if result not in obslogs:
                obslogs.append(result)

    return obslogs
