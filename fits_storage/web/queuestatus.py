"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, regular_summary, error_summary, error_detail, UnknownQueueError
from ..orm import session_scope
from .user import needs_login

from ..orm.ingestqueue import IngestQueue
from ..orm.exportqueue import ExportQueue
from ..orm.previewqueue import PreviewQueue
from ..orm.calcachequeue import CalCacheQueue

from . import templating

from mod_python import apache
from itertools import cycle

DETAIL_THRESHOLD = 20

@templating.templated("queuestatus/index.html", with_session=True)
def queuestatus_summary(session, req):
    general_rows = []
    detail_tables = {}

    for qstat in stats(session):
        general_rows.append(qstat)
        size, nerr = qstat['size'], qstat['errors']
        if size > 0 or nerr > 0:
            qname = qstat['name']
            if nerr > DETAIL_THRESHOLD:
                qname = qname + ' (limited to the first {})'.format(DETAIL_THRESHOLD)
            esummary = error_summary(session, qstat['type'], DETAIL_THRESHOLD)
            summary = regular_summary(session, qstat['type'], DETAIL_THRESHOLD)
            detail_tables[qstat['lname']] = {
                'lname':   qstat['lname'],
                'rows':    summary,
                'errors':  esummary
            }


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

QUEUELIMIT = 200

@needs_login(staffer=True)
def queuestatus(req, things):
    if len(things) > 1:
        if things[0] == 'json':
            return queuestatus_update(req, things)
        try:
            return queuestatus_tb(req, things[0], int(things[1]))
        except (TypeError, ValueError):
            # things[1] is not a valid integer, thus not an ID...
            pass
        except UnknownQueueError:
            # Something failed under queuestatus_tb...
            pass
    return queuestatus_summary(req)

queues = {
    'ingest': IngestQueue,
    'export': ExportQueue,
    'calcache': CalCacheQueue,
    'preview': PreviewQueue
    }

def queuestatus_update(req, things):
    try:
        queue = queues[things[1]]
    except KeyError:
        return apache.HTTP_NOT_FOUND

    most_recent, activity = None, False
    try:
        most_recent = int(things[2])
        activity = things[3] == 'active'
    except (ValueError, IndexError):
        pass

    if not most_recent:
        # Full query, probably the first one, the client side has no cached data
        pass

    return apache.HTTP_OK
