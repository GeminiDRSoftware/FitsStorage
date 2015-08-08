"""
This module contains the obslogs web summary code.
"""
import datetime

from ..orm import sessionfactory
from ..orm.obslog import Obslog
from ..fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from .list_headers import list_obslogs, list_headers
from .selection import sayselection, openquery
from ..apache_return_codes import HTTP_OK

from ..utils.userprogram import icanhave

from ..orm.querylog import QueryLog

def obslogs(req, selection, sumtype):
    """
    This is the obslogs summary generator
    """
    req.content_type = "text/html"
    req.write('<!DOCTYPE html><html>')
    req.write('<meta charset="UTF-8">')
    req.write('<link rel="stylesheet" href="/table.css">')
    title = "Obslogs table %s" % sayselection(selection)
    req.write("<title>%s</title>" % title)
    req.write("</head>\n")
    req.write("<body>")

    session = sessionfactory()
    try:
        # Instantiate querylog, populate initial fields
        querylog = QueryLog(req.usagelog)
        querylog.summarytype = sumtype
        querylog.selection = str(selection)
        querylog.query_started = datetime.datetime.utcnow()

        # If this is associated_obslogs, we do the association here
        if sumtype == 'associated_obslogs':
            querylog.add_note("Associated Obslogs")
            headers = list_headers(session, selection, None)
            obslogs = associate_obslogs(session, headers)
        else:
            # Simple obslog search
            obslogs = list_obslogs(session, selection, None)

        querylog.query_completed = datetime.datetime.utcnow()
        num_results = len(obslogs)
        querylog.numresults = num_results

        # Did we get any selection warnings?
        if 'warning' in selection.keys():
            req.write("<h3>WARNING: %s</h3>" % selection['warning'])
            querylog.add_note("Selection Warning: %s" % selection['warning'])
        # Note any notrecognised in the querylog
        if 'notrecognised' in selection.keys():
            querylog.add_note("Selection NotRecognised: %s" % selection['notrecognised'])
        # Note in the log if we hit limits
        if num_results == fits_open_result_limit:
            querylog.add_note("Hit Open search result limit")
        if num_results == fits_closed_result_limit:
            querylog.add_note("Hit Closed search result limit")

        # Did we get any results?
        if len(obslogs) > 0:
            # We have a session at this point, so get the user and their program list to
            # pass down the chain to use figure out whether to display download links
            obslog_table(req, obslogs, selection, session)
        else:
            # No results
            req.write('<H2>No Obslogs found</H2>')

        querylog.summary_completed = datetime.datetime.utcnow()

        # Add and commit the querylog
        session.add(querylog)
        session.commit()

    except IOError:
        pass
    finally:
        session.close()

    req.write("</body></html>")
    return HTTP_OK

def obslog_table(req, obslogs, selection, session):
    """
    Generates an HTML header summary table of obslogs
    the list of obslog objects provided.
    Writes that table to an apache request object.
    """

    # If the query was truncated, print a message saying so
    if openquery(selection) and len(obslogs) == fits_open_result_limit:
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit)
    elif len(obslogs) == fits_closed_result_limit:
        req.write('<P>WARNING: Your search generated more than the limit of %d results. Not all results have been shown. You might want to constrain your search more.</P>' % fits_closed_result_limit)

    # And tell them about clicking things
    req.write('<p>Click the filename to open the obslog in your browser. Right-click the filename or use your browser Save-Link-As function to download to a file. If the filename is not a link, you do not have access to that observation log - they follow the same proprietary period as the data they refer to.</p>')

    req.write('<TABLE class="fullwidth">')

    # Output the table header
    req.write('<TR class=tr_head>')
    req.write('<TH>Filename</TH>')
    req.write('<TH>UT Date</TH>')
    req.write('<TH>Program ID</TH>')
    req.write('</TR>')

    # Loop through the obslogs list, outputing table rows
    even = False
    for obslog in obslogs:
        even = not even
        tr_class = "tr_even" if even else "tr_odd"

        req.write('<TR class=%s>' % tr_class)

        if icanhave(session, req, obslog):
            req.write('<TD><a href="/file/%s" target="_blank">%s</a></TD>' % (obslog.diskfile.file.name, obslog.diskfile.file.name))
        else:
            req.write('<TD>%s</TD>' % obslog.diskfile.file.name)

        req.write('<TD>%s</TD>' % obslog.date)
        req.write('<TD>%s</TD>' % obslog.program_id)
        req.write('</TR>')

    req.write("</TABLE>\n")

    req.write('<a name="tableend"></a>')
    if openquery(selection) and len(obslogs) == fits_open_result_limit:
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit)
    elif len(obslogs) == fits_closed_result_limit:
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit)
    else:
        pass
        #url_prefix = "/download"
        #if sumtype == 'associated_obslogs':
            #url_prefix += '/associated_obslogs'
        #req.write('<P><a href="%s%s">Download all %d files.</a>.</P>' % (url_prefix, selection_to_URL(selection), len(obslogs)))


def associate_obslogs(session, headers):
    """
    Generate a list of obslogs that are associated with the given headers
    """

    obslogs = []

    # Could do this more efficiently by grouping the header query by date and progid, but this will do for now
    for header in headers:
        query = session.query(Obslog).filter(Obslog.date == header.ut_datetime.date()).filter(Obslog.program_id == header.program_id)
        results = query.all()
        for result in results:
            if result not in obslogs:
                obslogs.append(result)

    return obslogs
