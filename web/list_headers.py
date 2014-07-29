"""
This module contains the main list_headers function which is used for the web
summaries and a few other places to convert a selection dictionary into a
header object list by executing the query.
"""
from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from fits_storage_config import fits_open_result_limit, fits_closed_result_limit, use_as_archive
from web.selection import queryselection, openquery
from sqlalchemy import desc

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
    if orderby:
        for i in range(len(orderby)):
            if '_desc' in orderby[i]:
                orderby[i] = orderby[i].replace('_desc', '')
                if orderby[i] == 'filename':
                    query = query.order_by(desc('DiskFile.%s' % orderby[i]))
                if orderby[i] in whichorderby:
                    query = query.order_by(desc('Header.%s' % orderby[i]))
            else:
                if '_asc' in orderby[i]:
                    orderby[i] = orderby[i].replace('_asc', '')
                if orderby[i] == 'filename':
                    query = query.order_by('DiskFile.%s' % orderby[i])
                if orderby[i] in whichorderby:
                    query = query.order_by('Header.%s' % orderby[i])


    # By default we should order by filename, except for the archive, we should order by reverse date
    if use_as_archive:
        query.order_by(desc(Header.ut_datetime))
    else:
        query = query.order_by(File.name)

    # If this is an open query, we should limit the number of responses
    if openquery(selection):
        query = query.limit(fits_open_result_limit)
    else:
        query = query.limit(fits_closed_result_limit)

    headers = query.all()

    # Return the list of DiskFile objects
    return headers

