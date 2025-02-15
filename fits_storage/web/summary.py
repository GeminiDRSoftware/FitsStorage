"""
This module contains the main web summary code.
"""
import datetime
import json

from fits_storage.server.wsgi.context import get_context

from fits_storage.db.selection import Selection
from fits_storage.db.list_headers import list_headers

from .summary_generator import SummaryGenerator, NO_LINKS, FILENAME_LINKS, \
    ALL_LINKS, selection_to_column_names

from . import templating

from urllib.parse import quote_plus

from fits_storage.config import get_config
fsc = get_config()

from fits_storage.cal.associate_calibrations \
        import associate_cals_from_cache, associate_cals

from .userprogram import get_permissions_list

from fits_storage.server.orm.querylog import QueryLog


def summary(sumtype, selection, orderby, links=True, body_only=False):
    """
    This is the main summary generator.
    The main work is done by the summary_body() function.
    This function just wraps that in the relevant html
    tags to make it a page in its own right.
    """

    if body_only:
        return embeddable_summary(sumtype, selection, orderby, links)
    else:
        return full_page_summary(sumtype, selection, orderby, links)


@templating.templated("search_and_summary/summary.html", with_generator=True)
def full_page_summary(sumtype, selection, orderby, links):
    template_args = summary_body(sumtype, selection, orderby, links)

    template_args.update({
        'sumtype': sumtype,
        'sayselection': selection.say()
        })

    return template_args


@templating.templated("search_and_summary/summary_body.html",
                      with_generator=True)
def embeddable_summary(sumtype, selection, orderby, links):
    if selection:
        additional_columns = selection_to_column_names(selection)
    else:
        additional_columns = ()
    return summary_body(sumtype, selection, orderby, links,
                        additional_columns=additional_columns)


def summary_body(sumtype, selection, orderby, links=True,
                 additional_columns=()):
    """
    This is the main summary generator. sumtype is the summary type required.
    selection is a Selection() instance, simply passed through to the
    webhdrsummary function, orderby specifies how to order the output table,
    simply passed through to the webhdrsummary function

    This function outputs header and footer for the html page, and calls the
    webhdrsummary function to actually generate the html table containing the
    actual summary information.
    """

    ctx = get_context()
    session = ctx.session
    sumlinks = ALL_LINKS if links else NO_LINKS

    if ctx.env.method == 'POST':
        # extract list of files from request
        raw_data = get_context().req.raw_data
        data = raw_data.decode('utf-8')
        selection.update(json.loads(data))

    # If this is a diskfiles summary, select even ones that are not canonical
    if sumtype != 'diskfiles':
        # Usually, we want to only select headers with canonical diskfiles
        selection['canonical'] = True
    # Archive search results should only show files that are present,
    # so they can be downloaded
    if sumtype in {'searchresults', 'customsearch'}:
        selection['present'] = True

    # Instantiate querylog, populate initial fields
    querylog = QueryLog(ctx.usagelog)
    querylog.summarytype = sumtype
    querylog.selection = str(selection)
    querylog.query_started = datetime.datetime.utcnow()

    headers = list_headers(selection, orderby)
    num_headers = len(headers)

    fsc = get_config()
    hit_open_limit = num_headers == fsc.fits_open_result_limit
    hit_closed_limit = num_headers == fsc.fits_closed_result_limit

    querylog.query_completed = datetime.datetime.utcnow()
    querylog.numresults = num_headers
    # Did we get any selection warnings?
    if 'warning' in selection:
        querylog.add_note(f"Selection Warning: {selection['warning']}")
    # Note any notrecognised in the querylog
    if 'notrecognised' in selection.keys():
        querylog.add_note("Selection NotRecognised: %s" %
                          selection['notrecognised'])
    # Note in the log if we hit limits
    if hit_open_limit:
        querylog.add_note("Hit Open search result limit")
    if hit_closed_limit:
        querylog.add_note("Hit Closed search result limit")

    # If this is associated_cals, we do the association here
    if sumtype == 'associated_cals':
        querylog.add_note("Associated Cals")
        if fsc.using_calcache:
            headers = associate_cals_from_cache(session, headers)
        else:
            headers = associate_cals(session, headers)


        querylog.cals_completed = datetime.datetime.utcnow()
        querylog.numcalresults = len(headers)

        # links are messed up with associated_cals, turn most of them off
        if links:
            sumlinks = FILENAME_LINKS

    # Did we get any results?
    if len(headers) > 0:
        # We have a session at this point, so get the user and their program
        # list to pass down the chain to use figure out whether to display
        # download links
        user = ctx.user
        user_progid_list, user_obsid_list, user_file_list = \
            get_permissions_list(user)
        sumtable_data = summary_table(sumtype, headers, selection, sumlinks,
                                      user, user_progid_list, user_obsid_list,
                                      user_file_list, additional_columns)
    else:
        sumtable_data = {}

    querylog.summary_completed = datetime.datetime.utcnow()

    # Add and commit the querylog
    session.add(querylog)

    template_args =  dict(
        got_results      = sumtable_data,
        dev_system       = (sumtype not in {'searchresults', 'customsearch', 'associated_cals'}) and fsc.fits_system_status == 'development',
        open_query       = selection.openquery,
        hit_open_limit   = hit_open_limit,
        hit_closed_limit = hit_closed_limit,
        open_limit       = fsc.fits_open_result_limit,
        closed_limit     = fsc.fits_closed_result_limit,
        selection        = selection,
        calibrations     = True if sumtype == 'associated_cals' else False,
        **sumtable_data
        )

    return template_args

def summary_table(sumtype, headers, selection, links=ALL_LINKS, user=None, user_progid_list=None, user_obsid_list=None,
                  user_file_list=None, additional_columns=()):
    """
    Generates an HTML header summary table of the specified type from
    the list of header objects provided.

    sumtype: the summary type required
    headers: the list of header objects to include in the summary
    """

    ctx = get_context()

    # Construct the summary generator object.
    # If this is an ajax request and the type is searchresults, then
    # hack the uri to make it look like we came from searchform
    # so that the results point back to a form
    uri = ctx.env.uri
    uri = quote_plus(uri, safe='/=')
    if ctx.is_ajax and sumtype in {'searchresults', 'customsearch'}:
        uri = uri.replace("searchresults", "searchform")
        uri = uri.replace("customsearch", "searchform")

    url_prefix = "/download"
    if sumtype == 'associated_cals':
        url_prefix += '/associated_calibrations'

    sumgen = SummaryGenerator(sumtype, links, uri, user, user_progid_list, user_obsid_list, user_file_list,
                              additional_columns)

    download_all_url = f'{url_prefix}{selection.to_url()}'
    json_results_url = f'/jsonsummary{selection.to_url()}'

    class RowYielder(object):
        """
        Instances of this class are used by the summary template to iterate over the
        rows of data.

        These rows of data could be accessed directly, but there are a number of things
        to compute (total size, total downloadable files, etc.). This task is better
        done in the controller side of the process (this class), instead of in the
        view (template)

        The instance consumes its source data and once it has iterated over all the
        available header objects, it can only be used to query the totalized values.
        """
        def __init__(self, gen, headers, sumtype):
            self.gen     = gen
            self.sumtype = sumtype
            self.headers = iter(headers)
            self.bcount  = 0  # Byte count
            self.down    = 0  # Downloadable files
            self.total   = 0  # Total files

        @property
        def downloadable(self):
            "Used by the template to figure out if any of the results are downloadable"
            return self.down > 0

        @property
        def all_downloadable(self):
            "Convenience property for the template figure out if ALL results are downloadable"
            return self.total == self.down

        @property
        def size(self):
            return str(self.bcount)

        @property
        def size_in_gb(self):
            return '{:.2f}'.format(self.bcount / 1.0E9)

        def __iter__(self):
            return self

        def __next__(self):
            "Obtain the next row of data and keep some stats about it."
            header = next(self.headers)
            row = sumgen.table_row(header)
            # add row "type" to support tab-differentiation in our template
            row.sumtype = self.sumtype
            self.total = self.total + 1
            if row.can_download:
                self.down    = self.down + 1
                self.bcount += header.diskfile.file_size
            return row

    template_args = dict(
        clickable        = sumtype in {'searchresults', 'customsearch', 'associated_cals'},
        insert_prev      = sumtype in {'searchresults', 'customsearch'},
        uri              = sumgen.uri,
        deprogrammed_uri = sumgen.deprogrammed_uri,
        headers          = sumgen.table_header(),
        data_rows        = RowYielder(sumgen, headers, sumtype),
        cal_rows         = RowYielder(sumgen, headers, sumtype) if sumtype == 'associated_cals' else None,
        down_all_link    = download_all_url,
        json_res_link    = json_results_url,
        sumtype          = sumtype,
        )

    return template_args
