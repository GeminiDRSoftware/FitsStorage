"""
This module contains the programs web summary code.
"""
import datetime

from ..orm import NoResultFound
from ..orm.program import Program
from ..fits_storage_config import fits_system_status, fits_open_result_limit, fits_closed_result_limit
from .selection import sayselection, openquery
from .list_headers import list_programs

from . import templating

@templating.templated("programs/programs.html")
def programs(selection):
    """
    This is the programs summary generator
    """

    # Simple program search
    programs = list_programs(selection)

    # Did we get any selection warnings?
    #if 'warning' in selection:
        #querylog.add_note("Selection Warning: %s" % selection['warning'])
    # Note any notrecognised in the querylog
    #if 'notrecognised' in selection:
        #querylog.add_note("Selection NotRecognised: %s" % selection['notrecognised'])

    programs = list_programs(selection)

    # Construct suffix to html title
    things = []
    for thing in ['program_id', 'PIname', 'ProgramText']:
        if thing in selection:
            things.append(selection[thing])
    title_suffix = ' '.join(things)


    return dict(
        selection    = selection,
        title_suffix = title_suffix,
        no_programs  = len(programs) == 0,
        programs     = programs
        )
