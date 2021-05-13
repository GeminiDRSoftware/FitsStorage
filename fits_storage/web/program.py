"""
This module deals with displaying information about programs.
"""

from html import escape
from . import templating
from .templating import SkipTemplateError
from ..orm.program import Program

from ..utils.web import get_context, Return


@templating.templated("program.html")
def program_info(program_id):
    session = get_context().session

    ret_dict = { 'program_id': program_id }
    prog = session.query(Program).filter(Program.program_id==program_id).first()

    found = prog is not None

    ret_dict['not_found'] = not found

    if found:
        inames = prog.pi_coi_names.split(',')
        try:
            ret_dict['pi_name'] = escape(inames[0])
            ret_dict['co_names'] = escape(", ".join(inames[1:]))
        except (IndexError, TypeError):
            pass

        ret_dict['title'] = escape(prog.title)
        ret_dict['abstract'] = escape(prog.abstract)
        ret_dict['there_are_publications'] = len(prog.publications) > 0
        ret_dict['publications'] = prog.publications

    return ret_dict


@templating.templated("program.json")
def program_info_json(program_id):
    # TODO there is undoubtedly a cleaner way, but to do this quick just
    # ripping the html logic and added a json template.
    session = get_context().session

    ret_dict = { 'program_id': program_id }
    prog = session.query(Program).filter(Program.program_id==program_id).first()

    if prog is None:
        raise SkipTemplateError(Return.HTTP_NOT_FOUND)

    found = prog is not None

    ret_dict['not_found'] = not found

    if found:
        inames = prog.pi_coi_names.split(',')
        try:
            ret_dict['pi_name'] = escape(inames[0])
            ret_dict['co_names'] = escape(", ".join(inames[1:]))
        except (IndexError, TypeError):
            pass

        ret_dict['title'] = escape(prog.title)
        ret_dict['abstract'] = escape(prog.abstract)
        ret_dict['there_are_publications'] = len(prog.publications) > 0
        ret_dict['publications'] = prog.publications

    return ret_dict
