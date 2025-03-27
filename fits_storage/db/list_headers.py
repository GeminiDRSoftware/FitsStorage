"""
This module contains the main list_headers function which is used for the web
summaries and a few other places to convert a selection dictionary into a
header object list by executing the query.
"""
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header

from sqlalchemy import asc, desc, nullslast, func

from fits_storage.config import get_config

if get_config().is_server:
    from fits_storage.server.wsgi.context import get_context
    from fits_storage.server.orm.processingtag import ProcessingTag


def list_headers(selection, orderby, session=None, unlimit=False):
    """
    This function queries the database for a list of header table
    entries that satisfy the selection criteria.

    selection is a dictionary containing fields to select on
    orderby is a list of fields to sort the results by

    Returns a list of Header objects
    """

    if session is None:
        session = get_context().session

    # The basic query...
    query = session.query(Header).join(DiskFile).join(File)

    # Add the selection...
    query = selection.filter(query)

    # Do we have any order by arguments?

    whichorderby = ['instrument', 'data_label', 'observation_class',
                    'observation_type', 'airmass', 'ut_datetime', 'local_time',
                    'raw_iq', 'raw_cc', 'raw_bg', 'raw_wv', 'qa_state',
                    'filter_name', 'exposure_time', 'object', 'disperser',
                    'focal_plane_mask', 'ra', 'dec', 'detector_binning',
                    'central_wavelength']

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
            elif value == 'lastmod':
                order_criteria.append(sortingfunc(DiskFile.lastmod))
            elif value == 'entrytime':
                order_criteria.append(sortingfunc(DiskFile.entrytime))
            elif value in whichorderby:
                thing = getattr(Header, value)
                order_criteria.append(sortingfunc(thing))


    # Default sorting by ascending date if closed query, desc date if open query
    if selection.openquery:
        # On postgres, nulls default last on asc, and ordering by
        # nullslast(desc()) is very slow, *unless* there is an index
        # specifically to support it. There's a __table_args__ entry in the
        # Header ORM definition that adds this specific index.
        # We want NULLs to be last in *both* cases here.

        # order_criteria.append(desc(Header.ut_datetime))
        order_criteria.append(nullslast(desc(Header.ut_datetime)))
    else:
        # order_criteria.append(asc(Header.ut_datetime))
        order_criteria.append(nullslast(asc(Header.ut_datetime)))

    query = query.order_by(*order_criteria)

    # If this is an open query, we should limit the number of responses
    fsc = get_config()
    if not unlimit:
        if selection.openquery:
            query = query.limit(fsc.fits_open_result_limit)
        else:
            query = query.limit(fsc.fits_closed_result_limit)

    # Return the list of DiskFile objects
    return query.all()

def available_processing_tags(selection, session=None):
    """
    List the available processing tags for the selection. Cache the result
    in the selection object to for efficiency
    """
    if session is None:
        session = get_context().session

    # The basic query...
    query = session.query(Header.processing_tag).join(DiskFile).join(File)

    # Add the selection...
    query = selection.filter(query, ignore_processing_tag=True)

    query = query.group_by(Header.processing_tag)

    processing_tags = []
    for row in query.all():
        if row[0] is not None:
            processing_tags.append(row[0])

    selection.available_processing_tags = processing_tags
    return processing_tags

def default_processing_tags(selection, session=None):
    """
    List the default processing tags for the selection. Note, we return
    a list of tag values, not processing_tag ORM instances
    """
    if session is None:
        session = get_context().session

    # Get the available processing_tags. It may be stashed in the selction
    # object from a previous call
    if hasattr(selection, 'available_processing_tags'):
        available_tags = selection.available_processing_tags
    else:
        available_tags = available_processing_tags(selection, session=session)

    # This needs a subquery to do in SQL
    # SELECT id, domain, pri FROM
    #   (SELECT id, domain, pri, MAX(pri) OVER (PARTITION BY domain) AS maxpri FROM foo)
    # AS ss WHERE pri=maxpri;

    # For now hybrid solution so can manipulate in python at least untile we're
    # sure this is the long term solution. There will be a very small number of
    # records here, so there's no significant performance issue

    # Get domain, max_priority pairs
    dmpquery = session.query(ProcessingTag.domain,
                          func.max(ProcessingTag.priority)) \
        .filter(ProcessingTag.tag.in_(available_tags)) \
        .group_by(ProcessingTag.domain)

    tag_values = []
    for (domain, maxpri) in dmpquery.all():
        # Bear in mind there could be multiple tags for this domain with the
        # same priority - include them all
        dptags = session.query(ProcessingTag) \
            .filter(ProcessingTag.domain==domain) \
            .filter(ProcessingTag.priority==maxpri) \
            .all()
        for dptag in dptags:
            tag_values.append(dptag.tag)

    return tag_values
