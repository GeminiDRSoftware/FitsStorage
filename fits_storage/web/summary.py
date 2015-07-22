"""
This module contains the main web summary code.
"""
import datetime

from ..orm import sessionfactory
from ..fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from .selection import sayselection, openquery, selection_to_URL
from .list_headers import list_headers
from ..apache_return_codes as apache

from web.summary_generator import SummaryGenerator, htmlescape

# We assume that servers used as archive use a calibration association cache table
from ..fits_storage_config import use_as_archive
if use_as_archive:
    from ..cal.associate_calibrations import associate_cals_from_cache as associate_cals
else:
    from ..cal.associate_calibrations import associate_cals

from .user import userfromcookie
from .userprogram import get_program_list

from ..orm.querylog import QueryLog

def summary(req, sumtype, selection, orderby, links=True):
    """
    This is the main summary generator.
    The main work is done by the summary_body() function.
    This function just wraps that in the relevant html
    tags to make it a page in it's own right.
    """
    req.content_type = "text/html"
    req.write('<!DOCTYPE html><html>')
    req.write('<meta charset="UTF-8">')
    req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
    title = "FITS header %s table %s" % (sumtype, sayselection(selection))
    req.write("<title>%s</title>" % htmlescape(title))
    req.write("</head>\n")
    req.write("<body>")

    summary_body(req, sumtype, selection, orderby, links)

    req.write("</body></html>")
    return HTTP_OK

def summary_body(req, sumtype, selection, orderby, links=True):
    """
    This is the main summary generator.
    req is an apache request handler request object
    sumtype is the summary type required
    selection is an array of items to select on, simply passed
        through to the webhdrsummary function
    orderby specifies how to order the output table, simply
        passed through to the webhdrsummary function

    returns an apache request status code

    This function outputs header and footer for the html page,
    and calls the webhdrsummary function to actually generate
    the html table containing the actual summary information.
    """
    # In search results, warn about undefined stuff
    if 'notrecognised' in selection.keys():
        req.write("<H4>WARNING: I didn't recognize the following search terms: %s</H4>" % selection['notrecognised'])

    if sumtype not in ['searchresults', 'associated_cals']:
        if fits_system_status == "development":
            req.write('<h4>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h4>')
    # If this is a diskfiles summary, select even ones that are not canonical
    if sumtype != 'diskfiles':
        # Usually, we want to only select headers with diskfiles that are canonical
        selection['canonical'] = True
    # Archive search results should only show files that are present, so they can be downloaded
    if sumtype == 'searchresults':
        selection['present'] = True

    session = sessionfactory()
    try:
        # Instantiate querylog, populate initial fields
        querylog = QueryLog(req.usagelog)
        querylog.summarytype = sumtype
        querylog.selection = str(selection)
        querylog.query_started = datetime.datetime.utcnow()

        headers = list_headers(session, selection, orderby)
        num_headers = len(headers)
        querylog.query_completed = datetime.datetime.utcnow()
        querylog.numresults = num_headers
        # Did we get any selection warnings?
        if 'warning' in selection.keys():
            req.write("<h3>WARNING: %s</h3>" % selection['warning'])
            querylog.add_note("Selection Warning: %s" % selection['warning'])
        # Note any notrecognised in the querylog
        if 'notrecognised' in selection.keys():
            querylog.add_note("Selection NotRecognised: %s" % selection['notrecognised'])
        # Note in the log if we hit limits
        if num_headers == fits_open_result_limit:
            querylog.add_note("Hit Open search result limit")
        if num_headers == fits_closed_result_limit:
            querylog.add_note("Hit Closed search result limit")

        # If this is associated_cals, we do the association here
        if sumtype == 'associated_cals':
            querylog.add_note("Associated Cals")
            headers = associate_cals(session, headers)
            querylog.cals_completed = datetime.datetime.utcnow()
            querylog.numcalresults = len(headers)

            # links are messed up with associated_cals, turn them off
            links = False

        # Did we get any results?
        if len(headers) > 0:
            # We have a session at this point, so get the user and their program list to
            # pass down the chain to use figure out whether to display download links
            user = userfromcookie(session, req)
            user_progid_list = get_program_list(session, user)
            summary_table(req, sumtype, headers, selection, links, user, user_progid_list)
        else:
            # No results
            # Pass selection to this so it can do some helpful analysis of why you got no results
            no_results(req, selection)

        querylog.summary_completed = datetime.datetime.utcnow()

        # Add and commit the querylog
        session.add(querylog)
        session.commit()

    except IOError:
        pass
    finally:
        session.close()

def no_results(req, selection):
    """
    Print a helpful no results message
    """
    # We pass the selection dictionary to this function
    # and check for obvious mutually exclusive things
    req.write("<H2>Your search returned no results</H2>")
    req.write("<P>No data in the archive match your search criteria. Note that most searches (including program ID) are <b>exact match</b> searches, including only the first part of a program ID for example will not match any data. Also note that many combinations of search terms are in practice mutually exclusive - there will be no science BIAS frames for example, nor will there by any Imaging ARCs.</P>")
    req.write("<P>We suggest re-setting some of your constraints to <i>Any</i> and repeating your search.</P>")

    # Check for obvious mutually exclusive selections
    if 'observation_class' in selection.keys() and 'observation_type' in selection.keys():
        if selection['observation_class'] == 'science':
            if selection['observation_type'] in ['ARC', 'FLAT', 'DARK', 'BIAS']:
                req.write("<P>In this case, your combination of observation type and observation class is unlikely to find and data</P>")
    if 'inst' in selection.keys() and 'mode' in selection.keys():
        if selection['mode'] == 'MOS':
            if selection['inst'] not in ['GMOS', 'GMOS-N', 'GMOS-S', 'F2']:
                req.write("<P>Hint: %s does not support Multi-Object Spectroscopy</P>" % selection['inst'])
        if selection['mode'] == 'IFS':
            if selection['inst'] not in ['GMOS', 'GMOS-N', 'GMOS-S', 'GNIRS', 'NIFS', 'GPI']:
                req.write("<P>Hint: %s does not support Integral Field Spectroscopy</P>" % selection['inst'])
    if 'inst' in selection.keys() and 'spectroscopy' in selection.keys():
        if selection['spectroscopy'] == True:
            if selection['inst'] in ['NICI', 'GSAOI']:
                req.write("<P>Hint: %s is purely an imager - it does not do spectroscopy.</P>" % selection['inst'])
    # GNIRS XD central wavelength is not so useful
    if 'inst' in selection.keys() and 'disperser' in selection.keys() and 'central_wavelength' in selection.keys():
        if selection['inst'] == 'GNIRS' and 'XD' in selection['disperser']:
            req.write("<P>Hint - The central wavelength setting is not so useful with GNIRS cross-dispersed data because the spectral range is so big. Different central wavelength settings in the OT will come through in the headers and be respected by searches here, but in some cases it makes almost no difference to the actual light falling on the array. We suggest not setting central wavelength when you are searching for GNIRS XD data.</P>")


def summary_table(req, sumtype, headers, selection, links=True, user=None, user_progid_list=None):
    """
    Generates an HTML header summary table of the specified type from
    the list of header objects provided. Writes that table to an apache
    request object.

    req: the apache request object to write the output
    sumtype: the summary type required
    headers: the list of header objects to include in the summary
    """

    # Construct the summary generator object.
    # If this is an ajax request and the type is searchresults, then
    # hack the uri to make it look like we came from searchform
    # so that the results point back to a form
    uri = req.uri
    if isajax(req) and sumtype == 'searchresults':
        uri = uri.replace("searchresults", "searchform")

    sumgen = SummaryGenerator(sumtype, links, uri, user, user_progid_list)

    # If the query was open or truncated, print a message saying so, and disallow calibration association
    if openquery(selection) and len(headers) == fits_open_result_limit:
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. Calibration association will not be available. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit)
        req.write('<input type="hidden" id="allow_cals" value="no">')
    elif len(headers) == fits_closed_result_limit:
        req.write('<P>WARNING: Your search generated more than the limit of %d results. Not all results have been shown, and calibration association will not be available. You might want to constrain your search more.</P>' % fits_closed_result_limit)
        req.write('<input type="hidden" id="allow_cals" value="no">')
    else:
        req.write('<input type="hidden" id="allow_cals" value="yes">')

    if sumtype in ['searchresults', 'associated_cals']:
        # And tell them about clicking things
        req.write('<p>Click the [P] to preview an image of the data in your browser. Click the [D] to download that one file, use the check boxes to select a subset of the results to download, or if available a download all link is at <a href="#tableend"> the end of the table</a>. Click the filename to see the full header in a new tab. Click anything else to add that to your search criteria.</p>')
        req.write("<FORM action='/download' method='POST'>")

    if sumtype == 'searchresults':
        # Insert the preview box into the html
        req.write('<span id="previewbox">Click this box to close it. Click [P] links to switch image.<br /><img id="previewimage" src="/htmldocs/ajax-loading.gif" alt=""></span>')

    req.write('<TABLE class="fullwidth">')

    # Output the table header
    sumgen.table_header(req)

    # Loop through the header list, outputing table rows
    even = False
    bytecount = 0
    for header in headers:
        even = not even
        tr_class = ('tr_even' if even else 'tr_odd')

        sumgen.table_row(req, header, tr_class)

        bytecount += header.diskfile.file_size

    req.write("</TABLE>\n")

    if sumtype in ('searchresults', 'associated_cals'):
        req.write("<INPUT type='submit' value='Download Marked Files'>")
        req.write("</FORM>")

    req.write('<a name="tableend"></a>')
    if openquery(selection) and len(headers) == fits_open_result_limit:
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit)
    elif len(headers) == fits_closed_result_limit:
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit)
    else:
        url_prefix = "/download"
        if sumtype == 'associated_cals':
            url_prefix += '/associated_calibrations'
        req.write('<P><a href="%s%s">Download all %d files totalling %.2f GB</a>.</P>' % (url_prefix, selection_to_URL(selection), len(headers), bytecount/1.0E9))


def isajax(req):
    """
    Returns a boolean to say if the request came in via ajax
    """
    ajax = False
    if 'X-Requested-With' in req.headers_in.keys():
        value = req.headers_in['X-Requested-With']
        if value == 'XMLHttpRequest':
            ajax = True
    return ajax
