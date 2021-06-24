"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from gemini_obs_db.header import Header
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file import File
from .selection import sayselection, queryselection
from . import templating
from sqlalchemy import join, not_, func
import datetime

from ..gemini_metadata_utils import gemini_date

from ..utils.web import get_context

@templating.templated("progsobserved.html")
def progsobserved(selection):
    """
    This function generates a list of programs observed on a given night
    """

    if ("date" not in selection) and ("daterange" not in selection):
        selection["date"] = gemini_date("today")

    session = get_context().session

    # the basic query in this case
    query = session.query(Header.program_id).select_from(join(join(DiskFile, File), Header))

    # Add the selection criteria
    query = queryselection(query, selection)

    # Knock out null values. No point showing them as None for engineering files
    query = query.filter(Header.program_id != None)

    # And the group by clause
    progs_query = query.group_by(Header.program_id)

    return dict(
        selection = sayselection(selection),
        progs     = [p[0] for p in progs_query],
        joined_sel = '/'.join(list(selection.values()))
        )

@templating.templated("sitemap.xml", content_type='text/xml')
def sitemap(req):
    """
    This generates a sitemap.xml for google et al.
    We advertise a page for each program that we have data for... :-)
    """

    now = datetime.datetime.utcnow()
    year = datetime.timedelta(days=365).total_seconds()

    session = get_context().session

    # the basic query in this case
    query = session.query(Header.program_id, func.max(Header.ut_datetime)).group_by(Header.program_id)
    query = query.filter(not_(Header.program_id.contains('ENG'))).filter(not_(Header.program_id.contains('CAL')))

    items = []

    for prog, last in query:
        item = dict()
        item['prog'] = prog
        try:
            item['last'] = last.date().isoformat()
            interval = now - last
            if interval.total_seconds() < year:
                item['freq'] = 'weekly'
            else:
                item['freq'] = 'yearly'
            items.append(item)
        except AttributeError:
            pass

    return dict(items=items)
