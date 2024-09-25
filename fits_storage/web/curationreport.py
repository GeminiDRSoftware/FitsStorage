"""
This module contains the curation_report html generator function.
"""
from fits_storage.core.curation import duplicate_canonicals, \
    duplicate_present, present_not_canonical

from fits_storage.server.wsgi.context import get_context

from . import templating


@templating.templated("curation_report.html")
def curation_report():
    """
    Retrieves and prints out the desired values from the list created in
    FitsStorageCuration.py
    """
    ctx = get_context()

    if ctx.user is None or ctx.user.superuser is not True:
        return dict(allowed=False)

    session = ctx.session

    return dict(
        dup_canon=duplicate_canonicals(session),
        dup_pres=duplicate_present(session),
        pres_no_can=present_not_canonical(session)
        )
