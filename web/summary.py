"""
This module contains the main summary html generator function. 
"""
from orm import sessionfactory
from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from web.selection import sayselection, queryselection, openquery, selection_to_URL
from gemini_metadata_utils import GeminiDataLabel, percentilestring
import apache_return_codes as apache
from sqlalchemy import desc

from summary_generator import SummaryGenerator

def summary(req, type, selection, orderby, links=True, download=False):
    """
    This is the main summary generator.
    req is an apache request handler request object
    type is the summary type required
    selection is an array of items to select on, simply passed
        through to the webhdrsummary function
    orderby specifies how to order the output table, simply
        passed through to the webhdrsummary function

    returns an apache request status code

    This function outputs header and footer for the html page,
    and calls the webhdrsummary function to actually generate
    the html table containing the actual summary information.
    """
    req.content_type = "text/html"
    req.write('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd"><html>')
    title = "FITS header %s table %s" % (type, sayselection(selection))
    req.write("<head>")
    req.write("<title>%s</title>" % htmlescape(title))
    req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
    req.write("</head>\n")
    req.write("<body>")
    if (fits_system_status == "development"):
        req.write('<h4>This is the development system, please use <a href="http://fits/">fits</a> for operational use</h4>')
    if(type != 'searchresults'):
        req.write("<H1>%s</H1>\n" % htmlescape(title))
    else:
        # In search results, only warn about undefined stuff
        if('notrecognised' in selection.keys()):
            req.write("<H4>WARNING: I didn't recognize the following search terms: %s</H4>" % selection['notrecognised'])
        # And tell them about clicking things
        req.write('<p>Click the [D] to download that one file, use the check boxes to select a subset of the results to download, or if available a download all link is at <a href="#tableend"> the end of the table</a>. Click the filename to see the full header in a new tab. Click anything else to add that to your search criteria.</p>')

    # If this is a diskfiles summary, select even ones that are not canonical
    if(type != 'diskfiles'):
        # Usually, we want to only select headers with diskfiles that are canonical
        selection['canonical'] = True
    # Archive search results should only show files that are present, so they can be downloaded
    if(type == 'searchresults'):
        selection['present'] = True

    session = sessionfactory()
    try:
        summary_table(req, type, list_headers(session, selection, orderby), selection, links)
    except IOError:
        pass
    finally:
        session.close()

    req.write("</body></html>")
    return apache.OK

def summary_table(req, type, headers, selection, links=True):
    """
    Generates an HTML header summary table of the specified type from
    the list of header objects provided. Writes that table to an apache
    request object.

    req: the apache request object to write the output
    type: the summary type required
    headers: the list of header objects to include in the summary
    """

    # Construct the summary generator object.
    # If this is an ajax request and the type is searchresults, then
    # hack the uri to make it look like we came from searchform
    # so that the results point back to a form
    uri = req.uri
    if(isajax(req) and type == 'searchresults'):
        uri = uri.replace("searchresults", "searchform")
    sumgen = SummaryGenerator(type, links, uri)

    if(openquery(selection) and len(headers) == fits_open_result_limit):
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit) 
    elif(len(headers) == fits_closed_result_limit):
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit) 

    if(type == 'searchresults' and links == True):
        req.write("<FORM action='/download' method='POST'>")

    req.write('<TABLE class="fullwidth" border=0>')

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
 
    if(type == 'searchresults' and links == True):
        req.write("<INPUT type='submit' value='Download Marked Files'></INPUT>")
        req.write("</FORM>")

    req.write('<a name="tableend">')
    if(openquery(selection) and len(headers) == fits_open_result_limit):
        req.write('<P>WARNING: Your search does not constrain the number of results - ie you did not specify a date, date range, program ID etc. Searches like this are limited to %d results, and this search hit that limit. You may want to constrain your search. Constrained searches have a higher result limit.</P>' % fits_open_result_limit) 
    elif(len(headers) == fits_closed_result_limit):
        req.write('<P>WARNING: Your search generated more than the limit of %d results. You might want to constrain your search more.</P>' % fits_closed_result_limit) 
    else:
        req.write('<P><a href="/download%s">Download</a> all %d files totalling %.2f GB.</P>' % (selection_to_URL(selection), len(headers), bytecount/1.0E9))
    

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


    # If this is an open query, we should reverse sort by filename
    # and limit the number of responses
    if(openquery(selection)):
        query = query.order_by(desc(File.name))
        query = query.limit(fits_open_result_limit)
    else:
        # By default we should order by filename
        query = query.order_by(File.name)
        query = query.limit(fits_closed_result_limit)

    headers = query.all()
    
    # Return the list of DiskFile objects
    return headers

from cgi import escape
def htmlescape(string):
    """
    Convenience wrapper to cgi escape, providing type protection
    """

    if(type(string) in [str, unicode]):
        return escape(string)
    else:
        return None

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
