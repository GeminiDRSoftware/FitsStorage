"""
This module contains the programs web summary code.
"""
from fits_storage.db.list_headers import list_programs

from . import templating

@templating.templated("programs/programs.html")
def programs(selection):
    """
    This is the programs summary generator
    """
    prgms = list_programs(selection)

    # Construct suffix to html title
    things = []
    for thing in ['program_id', 'PIname', 'ProgramText']:
        if thing in selection:
            things.append(selection[thing])
    title_suffix = ' '.join(things)

    return dict(
        selection    = selection,
        title_suffix = title_suffix,
        no_programs  = len(prgms) == 0,
        programs     = prgms
        )
