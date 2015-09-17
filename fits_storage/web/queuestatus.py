"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, error_summary, error_detail, UnknownQueueError
from ..orm import session_scope
from .user import needs_login

from . import templating

from mod_python import apache
from itertools import cycle

DETAIL_THRESHOLD = 20

@templating.templated("queuestatus/index.html")
def queuestatus_summary(req):
    general_rows = []
    detail_tables = []

    with session_scope() as session:
        for qstat in stats(session):
            general_rows.append(qstat)
            nerr = qstat['errors']
            if nerr > 0:
                qname = qstat['name']
                if nerr > DETAIL_THRESHOLD:
                    qname = qname + ' (limited to the first {})'.format(DETAIL_THRESHOLD)
                summary = error_summary(session, qstat['type'], DETAIL_THRESHOLD)
                detail_tables.append(dict(name=qname,
                                          lname=qstat['lname'],
                                          rows=summary))

    template_args = dict(
        general_rows  = general_rows,
        detail_tables = detail_tables
        )

    return template_args

@templating.templated("queuestatus/detail.html")
def queuestatus_tb(req, qshortname, oid):
    with session_scope() as session:
        det = error_detail(session, qshortname, oid)

    template_args = dict(
        oid = oid,
        **det
        )

    return template_args

@needs_login(staffer=True)
def queuestatus(req, things):
    if len(things) > 1:
        try:
            return queuestatus_tb(req, things[0], int(things[1]))
        except (TypeError, ValueError):
            # things[1] is not a valid integer, thus not an ID...
            pass
        except UnknownQueueError:
            # Something failed under queuestatus_tb...
            pass
    return queuestatus_summary(req)
