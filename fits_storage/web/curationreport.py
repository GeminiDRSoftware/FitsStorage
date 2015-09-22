"""
This module contains the curation_report html generator function.
"""
from ..orm import sessionfactory
from ..orm.header import Header
from ..orm.curation import duplicate_canonicals, duplicate_present, present_not_canonical

from ..apache_return_codes import HTTP_OK

from . import templating

@templating.templated("curation_report.html", with_session=True)
def curation_report(session, req):
    """
    Retrieves and prints out the desired values from the list created in
    FitsStorageCuration.py
    """

    return dict(
        dup_canon   = duplicate_canonicals(session),
        dup_pres    = duplicate_present(session),
        pres_no_can = present_not_canonical(session)
        )
