"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from ..orm import session_scope
from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File
from .selection import sayselection, queryselection
from . import templating
from ..apache_return_codes import HTTP_OK
from sqlalchemy import join

@templating.templated("progsobserved.html")
def progsobserved(req, selection):
    """
    This function generates a list of programs observed on a given night
    """

    with session_scope() as session:
        try:
            # the basic query in this case
            query = session.query(Header.program_id).select_from(join(join(DiskFile, File), Header))

            # Add the selection criteria
            query = queryselection(query, selection)

            # And the group by clause
            progs_query = query.group_by(Header.program_id)

            return dict(
                selection = sayselection(selection),
                progs     = [p[0] for p in progs_query],
                joined_sel = '/'.join(selection.values())
                )
        except IOError:
            pass
