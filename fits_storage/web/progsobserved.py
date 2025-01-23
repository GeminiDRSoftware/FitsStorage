"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from sqlalchemy import join, func
import datetime

from fits_storage.core.orm.header import Header
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.file import File
from fits_storage.server.orm.publication import Publication
from fits_storage.gemini_metadata_utils import gemini_date
from fits_storage.server.wsgi.context import get_context
from . import templating


@templating.templated("progsobserved.html")
def progsobserved(selection):
    """
    This function generates a list of programs observed on a given night
    """

    if ("date" not in selection) and ("daterange" not in selection):
        selection["date"] = gemini_date("today")

    session = get_context().session

    # the basic query in this case
    query = session.query(Header.program_id)\
        .select_from(join(join(DiskFile, File), Header))

    # Add the selection criteria
    query = selection.filter(query)

    # Knock out null values. No point showing them as None for engineering files
    query = query.filter(Header.program_id != None)

    # And the group by clause
    progs_query = query.group_by(Header.program_id)

    return dict(
        selection = selection.say(),
        progs = [p[0] for p in progs_query],
        joined_sel = '/'.join(list(selection.values()))
        )


@templating.templated("sitemap.xml", content_type='text/xml')
def sitemap():
    """
    This generates a sitemap.xml for Google et al.
    We advertise a page for each program that we have data for... :-)
    """

    now = datetime.datetime.utcnow()
    year = datetime.timedelta(days=365).total_seconds()

    session = get_context().session

    items = []

    # query for the programs
    query = session.query(Header.program_id, func.max(Header.ut_datetime))\
        .join(DiskFile).filter(DiskFile.canonical == True)\
        .group_by(Header.program_id)\
        .filter(Header.engineering == False)\
        .filter(Header.calibration_program == False)\
        .filter(Header.ut_datetime != None)

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

    # query for publications
    query = session.query(Publication)

    # This is a little kludgey. The template outputs searchform/{{ item.prog }}
    for publication in query:
        item = dict()
        item['prog'] = f'publication={publication.bibcode}'

        program_ids = []
        for program in publication.programs:
            program_ids.append(program.program_id)

        last = session.query(func.max(Header.ut_datetime)) \
            .join(DiskFile).filter(DiskFile.canonical == True)\
            .filter(Header.program_id.in_(program_ids))\
            .filter(Header.ut_datetime != None)\
            .first()
        if last and last[0]:
            last = last[0]
            item['last'] = last.date().isoformat()
            interval = now - last
            if interval.total_seconds() < year:
                item['freq'] = 'weekly'
            else:
                item['freq'] = 'yearly'
        items.append(item)

    return dict(items=items)
