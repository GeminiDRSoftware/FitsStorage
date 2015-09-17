"""
This module contains the main web summary code.
"""
import datetime

from ..orm import session_scope
from ..fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from .selection import sayselection, openquery, selection_to_URL
from .list_headers import list_headers
from .. import apache_return_codes as apache

from .summary_generator import SummaryGenerator, htmlescape, NO_LINKS, FILENAME_LINKS, ALL_LINKS

from . import templating

# We assume that servers used as archive use a calibration association cache table
from ..fits_storage_config import use_as_archive
if use_as_archive:
    from ..cal.associate_calibrations import associate_cals_from_cache as associate_cals
else:
    from ..cal.associate_calibrations import associate_cals

from .user import userfromcookie
from .userprogram import get_program_list

from ..orm.querylog import QueryLog

def summary(req, sumtype, selection, orderby, links=True, body_only=False):
    """
    This is the main summary generator.
    The main work is done by the summary_body() function.
    This function just wraps that in the relevant html
    tags to make it a page in it's own right.
    """

    req.content_type = "text/html"


    with session_scope() as session:
        template_args = summary_body(req, session, sumtype, selection, orderby, links)
        if body_only:
            template = templating.get_env().get_template('search_and_summary/summary_body.html')
        else:
            template = templating.get_env().get_template('search_and_summary/summary.html')

            template_args.update({
                'sumtype'      : sumtype,
                'sayselection' : sayselection(selection)
                })

        for text in template.generate(template_args):
            req.write(text)

    return apache.HTTP_OK

def summary_body(req, session, sumtype, selection, orderby, links=True):
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

    sumlinks = ALL_LINKS if links else NO_LINKS

    # If this is a diskfiles summary, select even ones that are not canonical
    if sumtype != 'diskfiles':
        # Usually, we want to only select headers with diskfiles that are canonical
        selection['canonical'] = True
    # Archive search results should only show files that are present, so they can be downloaded
    if sumtype == 'searchresults':
        selection['present'] = True

    try:
        # Instantiate querylog, populate initial fields
        querylog = QueryLog(req.usagelog)
        querylog.summarytype = sumtype
        querylog.selection = str(selection)
        querylog.query_started = datetime.datetime.utcnow()

        headers = list_headers(session, selection, orderby, full_query=True)
        num_headers = len(headers)

        hit_open_limit = num_headers == fits_open_result_limit
        hit_closed_limit = num_headers == fits_closed_result_limit

        querylog.query_completed = datetime.datetime.utcnow()
        querylog.numresults = num_headers
        # Did we get any selection warnings?
        if 'warning' in selection:
            querylog.add_note("Selection Warning: {}".format(selection['warning']))
        # Note any notrecognised in the querylog
        if 'notrecognised' in selection.keys():
            querylog.add_note("Selection NotRecognised: %s" % selection['notrecognised'])
        # Note in the log if we hit limits
        if hit_open_limit:
            querylog.add_note("Hit Open search result limit")
        if hit_closed_limit:
            querylog.add_note("Hit Closed search result limit")

        # If this is associated_cals, we do the association here
        if sumtype == 'associated_cals':
            querylog.add_note("Associated Cals")
            headers = associate_cals(session, (x[0] for x in headers), full_query=True)
            querylog.cals_completed = datetime.datetime.utcnow()
            querylog.numcalresults = len(headers)

            # links are messed up with associated_cals, turn most of them off
            if links:
                sumlinks = FILENAME_LINKS

        # Did we get any results?
        if len(headers) > 0:
            # We have a session at this point, so get the user and their program list to
            # pass down the chain to use figure out whether to display download links
            user = userfromcookie(session, req)
            user_progid_list = get_program_list(session, user)
            sumtable_data = summary_table(req, sumtype, headers, selection, sumlinks, user, user_progid_list)
        else:
            sumtable_data = {}

        querylog.summary_completed = datetime.datetime.utcnow()

        # Add and commit the querylog
        session.add(querylog)

        return dict(
            got_results      = sumtable_data,
            dev_system       = (sumtype not in {'searchresults', 'associated_cals'}) and fits_system_status == 'development',
            open_query       = openquery(selection),
            hit_open_limit   = hit_open_limit,
            hit_closed_limit = hit_closed_limit,
            open_limit       = fits_open_result_limit,
            closed_limit     = fits_closed_result_limit,
            selection        = selection,
            **sumtable_data
            )

    except IOError:
        pass


def summary_table(req, sumtype, headers, selection, links=ALL_LINKS, user=None, user_progid_list=None):
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

    url_prefix = "/download"
    if sumtype == 'associated_cals':
        url_prefix += '/associated_calibrations'

    download_all_url = '{}{}'.format(url_prefix, selection_to_URL(selection))

    class RowYielder(object):
        def __init__(self, gen, headers):
            self.gen     = gen
            self.headers = iter(headers)
            self.bcount  = 0  # Byte count
            self.down    = 0  # Downloadable files
            self.total   = 0  # Total files

        @property
        def downloadable(self):
            return self.down

        @property
        def all_downloadable(self):
            return self.total == self.down

        @property
        def size(self):
            return str(self.bcount)

        @property
        def size_in_gb(self):
            return '{:.2f}'.format(self.bcount / 1.0E9)

        def __iter__(self):
            return self

        def next(self):
            header = self.headers.next()
            row = sumgen.table_row(*header)
            self.total = self.total + 1
            if row.can_download:
                self.down    = self.down + 1
                self.bcount += header[1].file_size

            return row

        # For future Python 3 compliance
        __next__ = next

    template = templating.get_env().get_template('search_and_summary/summary_table.html')

    template_args = dict(
        clickable     = sumtype in {'searchresults', 'associated_cals'},
        insert_prev   = sumtype == 'searchresults',
        headers       = sumgen.table_header(),
        data_rows     = RowYielder(sumgen, headers),
        down_all_link = download_all_url,
        )

    return template_args


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
