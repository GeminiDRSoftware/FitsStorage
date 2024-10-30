"""
This module contains the main list_headers function which is used for the web
summaries and a few other places to convert a selection dictionary into a
header object list by executing the query.
"""
from fits_storage.core.orm.file import File
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.server.orm.program import Program
from fits_storage.server.orm.obslog import Obslog
from .selection import Selection
from fits_storage.gemini_metadata_utils import gemini_date, \
    gemini_daterange, get_time_period
from sqlalchemy import asc, desc, func, nullslast, or_

from fits_storage.server.wsgi.context import get_context

from fits_storage.config import get_config


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


def list_obslogs(selection, orderby):
    """
    This function searches the database for a list of obslog table
    entries that satisfy the selection criteria

    selection is a dictionary containing fields to select on
    orderby is a list of fields to sort the results by

    The only fields used in the selection are date, daterange and program ID

    Returns a list of Obslog objects
    """

    # The basic query
    session = get_context().session

    query = session.query(Obslog).select_from(Obslog, DiskFile)\
        .filter(Obslog.diskfile_id == DiskFile.id)

    # Cant use queryselection as that assumes it's a header object.
    # Just do it here.

    if 'date' in selection:
        date = gemini_date(selection['date'], as_date=True)
        query = query.filter(Obslog.date == date)

    if 'daterange' in selection:
        # Get parsed start and end datetime objects
        daterange = selection['daterange']
        startd, endd = gemini_daterange(selection['daterange'],
                                        as_dates=True)
        if startd is None or endd is None:
            raise ValueError('Not a valid daterange: {0}'.format(daterange))

        startdt, enddt = get_time_period(startd, endd)

        # check it's between these two
        query = query.filter(Obslog.date >= startdt).filter(Obslog.date < enddt)

    if 'program_id' in selection:
        query = query.filter(Obslog.program_id == selection['program_id'])

    if 'filename' in selection:
        fn = selection.get('filename')
        if fn.endswith('.bz2'):
            ofn = fn.rstrip('.bz2')
        else:
            ofn = fn + '.bz2'
        query = query.filter(or_(DiskFile.filename == fn,
                                 DiskFile.filename == ofn))

    # Order by inverse date for now
    query = query.order_by(desc(Obslog.date))

    # Limit to 1000 for now
    query = query.limit(1000)

    # Get the list and return it

    return query.all()


def list_programs(selection):
    """
    This function searches the database for a list of program table
    entries that satisfy the selection criteria

    selection is a dictionary containing fields to select on

    The only fields used in the selection are programid,

    Returns a list of Program objects
    """

    # The basic query
    query = get_context().session.query(Program)

    # Can't use queryselection as that assumes header objects
    # Build the query here manually

    if 'program_id' in selection:
        query = query.filter(Program.program_id == selection['program_id'])

    if 'PIname' in selection:
        query = query.filter(
            func.to_tsvector(Program.pi_coi_names)
            .match(' & '.join(selection['PIname'].split()))
        )

    if 'ProgramText' in selection:
        query = query.filter(
            func.to_tsvector(Program.title)
            .match(' & '.join(selection['ProgramText'].split()))
        )

    return query.all()
