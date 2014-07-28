"""
This module contains the main web summary code. 
"""
from orm import sessionfactory
from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit, use_as_archive
from web.selection import sayselection, queryselection, openquery, selection_to_URL
import apache_return_codes as apache
from sqlalchemy import desc

from web.summary_generator import SummaryGenerator, htmlescape
from cal.associate_calibrations import associate_cals

from web.user import userfromcookie
from web.userprogram import get_program_list

def summary(req, sumtype, selection, orderby, links=True):
    """
    This is the main summary generator.
    The main work is done by the summary_body() funciton.
    This funciton just wraps that in the relevant html
    tags to make it a page in it's own right.
    """
    req.content_type = "text/html"
    req.write('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd"><html>')
    title = "FITS header %s table %s" % (sumtype, sayselection(selection))
    req.write("<head>")
    req.write("<title>%s</title>" % htmlescape(title))
    req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
    req.write("</head>\n")
    req.write("<body>")

    summary_body(req, sumtype, selection, orderby, links)

    req.write("</body></html>")
    return apache.OK

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
    if('notrecognised' in selection.keys()):
        req.write("<H4>WARNING: I didn't recognize the following search terms: %s</H4>" % selection['notrecognised'])

    if(sumtype != 'searchresults'):
        if (fits_system_status == "development"):
            req.write('<h4>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h4>')
    # If this is a diskfiles summary, select even ones that are not canonical
    if(sumtype != 'diskfiles'):
        # Usually, we want to only select headers with diskfiles that are canonical
        selection['canonical'] = True
    # Archive search results should only show files that are present, so they can be downloaded
    if(sumtype == 'searchresults'):
        selection['present'] = True

    session = sessionfactory()
    try:
        headers = list_headers(session, selection, orderby)
        # Did we get any selection warnings?
        if 'warning' in selection.keys():
            req.write("<h3>WARNING: %s</h3>" % selection['warning'])

        # If this is assocated_cals, we do the assoication here
        if(sumtype == 'associated_cals'):
            headers = associate_cals(session, headers)

            # links are messed up with associated_cals, turn them off
            links = False
        
        # Did we get any results?
        if(len(headers) > 0):
            # We have a session at this point, so get the user and their program list to 
            # pass down the chain to use figure out whether to display download links
            user = userfromcookie(session, req)
            user_progid_list = get_program_list(session, user)
            summary_table(req, sumtype, headers, selection, links, user, user_progid_list)
        else:
            # No results
            # Pass selection to this so it can do some helpful analysis of why you got no results
            no_results(req, selection)

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
    req.write("<P>No data in the archive match your search criteria. Note that the program ID and Target Name searches are <b>exact match</b> searches, including only the first part of a program ID for example will not match any data. Also note that many combinations of search terms are in practice mutually exclusive - there will be no science BIAS frames for example, nor will there by any Imaging ARCs.</P>")
    req.write("<P>We suggest re-setting some of your constraints to <i>Any</i> and repeating your search.</P>")

    # Check for obvious mutually exclusive selections
    if('observation_class' in selection.keys() and 'observation_type' in selection.keys()):
        if (selection['observation_class'] == 'science'):
            if(selection['observation_type'] in ['ARC', 'FLAT', 'DARK', 'BIAS']):
                req.write("<P>In this case, your combination of observation type and observation class is unlikely to find and data</P>")
    if('inst' in selection.keys() and 'mode' in selection.keys()):
        if(selection['mode'] == 'MOS'):
            if(selection['inst'] not in ['GMOS', 'GMOS-N', 'GMOS-S', 'F2']):
                req.write("<P>Hint: %s does not support Multi-Object Spectroscopy</P>" % selection['inst'])
        if(selection['mode'] == 'IFS'):
            if(selection['inst'] not in ['GMOS', 'GMOS-N', 'GMOS-S', 'GNIRS', 'NIFS', 'GPI']):
                req.write("<P>Hint: %s does not support Integral Field Spectroscopy</P>" % selection['inst'])
    if('inst' in selection.keys() and 'spectroscopy' in selection.keys()):
        if(selection['spectroscopy'] == True):
            if(selection['inst'] in ['NICI', 'GSAOI']):
                req.write("<P>Hint: %s is purely an imager - it does not do spectroscopy.</P>" % selection['inst'])


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
    if(isajax(req) and sumtype == 'searchresults'):
        uri = uri.replace("searchresults", "searchform")

    sumgen = SummaryGenerator(sumtype, links, uri, user, user_progid_list)

    if(openquery(selection) and len(headers) == fits_open_result_limit):
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit) 
    elif(len(headers) == fits_closed_result_limit):
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit) 

    if(sumtype in ['searchresults', 'associated_cals']):
        # And tell them about clicking things
        req.write('<p>Click the [D] to download that one file, use the check boxes to select a subset of the results to download, or if available a download all link is at <a href="#tableend"> the end of the table</a>. Click the filename to see the full header in a new tab. Click anything else to add that to your search criteria.</p>')
        req.write("<FORM action='/download' method='POST'>")

    req.write('<TABLE class="fullwidth">')

    # Output the table header
    sumgen.table_header(req)

    # Loop through the header list, outputing table rows
    even = False
    bytecount = 0
    for header in headers:
        even = not even
        if(even):
            tr_class = "tr_even"
        else:
            tr_class = "tr_odd"

        sumgen.table_row(req, header, tr_class)

        bytecount += header.diskfile.file_size

    req.write("</TABLE>\n")
 
    if(sumtype in ['searchresults', 'associated_cals']):
        req.write("<INPUT type='submit' value='Download Marked Files'>")
        req.write("</FORM>")

    req.write('<a name="tableend"></a>')
    if(openquery(selection) and len(headers) == fits_open_result_limit):
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit) 
    elif(len(headers) == fits_closed_result_limit):
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit) 
    else:
        req.write('<P><a href="/download%s">Download all %d files totalling %.2f GB</a>.</P>' % (selection_to_URL(selection), len(headers), bytecount/1.0E9))
    

def list_headers(session, selection, orderby):
    """
    This function queries the database for a list of header table 
    entries that satisfy the selection criteria.

    session is an sqlalchemy session on the database
    selection is a dictionary containing fields to select on
    orderby is a list of fields to sort the results by

    Returns a list of Header objects
    """
    # The basic query...
    query = session.query(Header).select_from(Header, DiskFile, File)
    query = query.filter(Header.diskfile_id == DiskFile.id)
    query = query.filter(DiskFile.file_id == File.id)
    query = queryselection(query, selection)

    # Do we have any order by arguments?

    whichorderby = ['instrument', 'data_label', 'observation_class', 'airmass', 'ut_datetime', 'local_time', 'raw_iq', 'raw_cc', 'raw_bg', 'raw_wv', 'qa_state', 'filter_name', 'exposure_time', 'object']
    if (orderby):
        for i in range(len(orderby)):
            if '_desc' in orderby[i]:
                orderby[i] = orderby[i].replace('_desc', '')
                if (orderby[i] == 'filename'):
                    query = query.order_by(desc('DiskFile.%s' % orderby[i]))
                if orderby[i] in whichorderby:
                    query = query.order_by(desc('Header.%s' % orderby[i]))
            else:
                if '_asc' in orderby[i]:
                    orderby[i] = orderby[i].replace('_asc', '')
                if (orderby[i] == 'filename'):
                    query = query.order_by('DiskFile.%s' % orderby[i])
                if orderby[i] in whichorderby:
                    query = query.order_by('Header.%s' % orderby[i])


    # By default we should order by filename, except for the archive, we should order by reverse date
    if use_as_archive:
        query.order_by(desc(Header.ut_datetime))
    else:
        query = query.order_by(File.name)

    # If this is an open query, we should limit the number of responses
    if(openquery(selection)):
        query = query.limit(fits_open_result_limit)
    else:
        query = query.limit(fits_closed_result_limit)

    headers = query.all()
    
    # Return the list of DiskFile objects
    return headers

def isajax(req):
    """
    Returns a boolean to say if the request came in via ajax
    """
    ajax = False
    if('X-Requested-With' in req.headers_in.keys()):
        value = req.headers_in['X-Requested-With']
        if(value == 'XMLHttpRequest'):
            ajax = True
    return ajax
