"""
This module contains the main list_headers function which is used for the web
summaries and a few other places to convert a selection dictionary into a
header object list by executing the query.
"""
from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.preview import Preview
from ..orm.header import Header
from ..orm.obslog import Obslog
from ..fits_storage_config import fits_open_result_limit, fits_closed_result_limit, use_as_archive
from .selection import queryselection, openquery
from ..gemini_metadata_utils import gemini_date, gemini_time_period_from_range
from sqlalchemy import asc, desc
import dateutil.parser

from ..utils.web import Context

def list_headers(selection, orderby, full_query=False, add_previews=False):
    """
    This function queries the database for a list of header table
    entries that satisfy the selection criteria.

    session is an sqlalchemy session on the database
    selection is a dictionary containing fields to select on
    orderby is a list of fields to sort the results by

    Returns a list of Header objects
    """

    session = Context().session

    # The basic query...
    if full_query:
        if add_previews:
            query = session.query(Header, DiskFile, File, Preview).join(DiskFile).join(File).outerjoin(Preview)
        else:
            query = session.query(Header, DiskFile, File).join(DiskFile).join(File)
    else:
        query = session.query(Header).join(DiskFile).join(File)
    query = queryselection(query, selection)


    # Do we have any order by arguments?

    whichorderby = ['instrument', 'data_label', 'observation_class', 'observation_type', 'airmass', 'ut_datetime', 'local_time',
                    'raw_iq', 'raw_cc', 'raw_bg', 'raw_wv', 'qa_state', 'filter_name', 'exposure_time', 'object', 'disperser', 
                    'focal_plane_mask', 'ra', 'dec', 'detector_binning', 'central_wavelength']

    order_criteria = []
    if orderby:
        for value in orderby:
            sortingfunc = asc
            if '_desc' in value:
                value = value.replace('_desc', '')
                sortingfunc = desc
            if '_asc' in value:
                value = value.replace('_asc', '')

            if value == 'filename':
                order_criteria.append(sortingfunc(DiskFile.filename))
            elif value in whichorderby:
                thing = getattr(Header, value)
                order_criteria.append(sortingfunc(thing))



    is_openquery = openquery(selection)

    # Default sorting by ascending date if closed query, desc date if open query
    if is_openquery:
        order_criteria.append(desc(Header.ut_datetime))
    else:
        order_criteria.append(asc(Header.ut_datetime))

    query = query.order_by(*order_criteria)

    # If this is an open query, we should limit the number of responses
    if is_openquery:
        query = query.limit(fits_open_result_limit)
    else:
        query = query.limit(fits_closed_result_limit)

    # Return the list of DiskFile objects
    return query.all()


def list_obslogs(session, selection, orderby):
    """
    This function searches the database for a list of obslog table
    entries that satisfy the selection criteria

    session is an sqlalchemy session on the database
    selection is a dictionary containing fields to select on
    orderby is a list of fields to sort the results by

    The only fields used in the selection are date, daterange and program ID

    Returns a list of Obslog objects
    """

    # The basic query
    query = session.query(Obslog).select_from(Obslog, DiskFile)\
                    .filter(Obslog.diskfile_id == DiskFile.id)

    # Cant use queryselection as that assumes it's a header object.
    # Just do it here.

    if 'date' in selection:
        date = gemini_date(selection['date'], as_datetime=True).date()
        query = query.filter(Obslog.date == date)

    if 'daterange' in selection:
        # Get parsed start and end datetime objects
        daterange = selection['daterange']
        try:
            start, end = gemini_time_period_from_range(daterange)
        except (TypeError, ValueError):
            raise ValueError('Not a valid daterange: {0}'.format(daterange))

        # check it's between these two
        query = query.filter(Obslog.date >= start).filter(Obslog.date < end)

    if 'program_id' in selection:
        query = query.filter(Obslog.program_id == selection['program_id'])


    # Order by inverse date for now
    query = query.order_by(desc(Obslog.date))

    # Limit to 1000 for now
    query = query.limit(1000)

    # Get the list and return it

    return query.all()


