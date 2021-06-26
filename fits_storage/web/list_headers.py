"""
This module contains the main list_headers function which is used for the web
summaries and a few other places to convert a selection dictionary into a
header object list by executing the query.
"""
from gemini_obs_db.file import File
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.preview import Preview
from gemini_obs_db.header import Header
from ..orm.program import Program
from gemini_obs_db.provenance import Provenance
from ..orm.obslog import Obslog
from ..orm.obslog_comment import ObslogComment
from ..fits_storage_config import fits_open_result_limit, fits_closed_result_limit, use_as_archive
from .selection import queryselection, openquery
from gemini_obs_db.utils.gemini_metadata_utils import gemini_date, gemini_time_period_from_range
from sqlalchemy import asc, desc, func
import dateutil.parser

from ..utils.web import get_context

def list_headers(selection, orderby, full_query=False, add_previews=False, session=None, unlimit=False):
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
    if full_query:
        if add_previews:
            # query = session.query(Header, DiskFile, File, ObslogComment, Preview).join(DiskFile).join(File, DiskFile.file_id == File.id).filter(Header.diskfile_id == DiskFile.id).outerjoin(ObslogComment, Header.data_label == ObslogComment.data_label).outerjoin(Preview, Preview.diskfile_id == DiskFile.id)
            query = session.query(Header, DiskFile, File, ObslogComment).join(DiskFile, Header.diskfile_id == DiskFile.id).join(File, DiskFile.file_id == File.id).filter(Header.diskfile_id == DiskFile.id).outerjoin(Provenance).outerjoin(ObslogComment, Header.data_label == ObslogComment.data_label).outerjoin(Preview, Preview.diskfile_id == DiskFile.id)
        else:
            # query = session.query(Header, DiskFile, File, ObslogComment).join(DiskFile).join(File).outerjoin(ObslogComment, Header.data_label == ObslogComment.data_label)
            query = session.query(Header, DiskFile, File, ObslogComment).join(DiskFile, Header.diskfile_id == DiskFile.id).join(File, DiskFile.file_id == File.id).filter(Header.diskfile_id == DiskFile.id).outerjoin(Provenance).outerjoin(ObslogComment, Header.data_label == ObslogComment.data_label)
    else:
        query = session.query(Header).join(DiskFile).join(File).outerjoin(Provenance)
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
            elif value == 'lastmod':
                order_criteria.append(sortingfunc(DiskFile.lastmod))
            elif value == 'entrytime':
                order_criteria.append(sortingfunc(DiskFile.entrytime))
            elif value in whichorderby:
                thing = getattr(Header, value)
                order_criteria.append(sortingfunc(thing))



    is_openquery = openquery(selection)

    # Default sorting by ascending date if closed query, desc date if open query
    if is_openquery:
        # This makes the query extremely slow on ops
        # order_criteria.append(nullslast(desc(Header.ut_datetime)))
        order_criteria.append(desc(Header.ut_datetime))
    else:
        order_criteria.append(asc(Header.ut_datetime))

    query = query.order_by(*order_criteria)

    # If this is an open query, we should limit the number of responses
    if not unlimit:
        if is_openquery:
            query = query.limit(fits_open_result_limit)
        else:
            query = query.limit(fits_closed_result_limit)

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
        query = query.filter(Program.program_id==selection['program_id'])

    if 'PIname' in selection:
        query = query.filter(
            func.to_tsvector(Program.pi_coi_names).match(' & '.join(selection['PIname'].split()))
        )

    if 'ProgramText' in selection:
        query = query.filter(
            func.to_tsvector(Program.title).match(' & '.join(selection['ProgramText'].split()))
        )

    return query.all()
