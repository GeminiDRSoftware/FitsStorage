from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.server.orm.obslog import Obslog
from fits_storage.gemini_metadata_utils import gemini_date, \
    gemini_daterange, get_time_period
from sqlalchemy import desc, or_

from fits_storage.server.wsgi.context import get_context

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
