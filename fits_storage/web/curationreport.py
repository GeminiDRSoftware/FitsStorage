"""
This module contains the curation_report html generator function.
"""
from gemini_obs_db.header import Header
from ..orm.curation import duplicate_canonicals, duplicate_present, present_not_canonical

from ..utils.web import get_context

from . import templating

@templating.templated("curation_report.html")
def curation_report():
    """
    Retrieves and prints out the desired values from the list created in
    FitsStorageCuration.py
    """

    session = get_context().session

    return dict(
        dup_canon   = duplicate_canonicals(session),
        dup_pres    = duplicate_present(session),
        pres_no_can = present_not_canonical(session)
        )
