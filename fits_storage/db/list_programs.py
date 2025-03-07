from fits_storage.server.orm.program import Program

from sqlalchemy import func

from fits_storage.server.wsgi.context import get_context

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
