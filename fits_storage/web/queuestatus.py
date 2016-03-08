"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, regular_summary, error_summary, error_detail, UnknownQueueError
from ..utils.web import Context

from ..orm import session_scope
from .user import needs_login

from ..orm.ingestqueue import IngestQueue
from ..orm.exportqueue import ExportQueue
from ..orm.previewqueue import PreviewQueue
from ..orm.calcachequeue import CalCacheQueue

from . import templating

from mod_python import apache
import json
from hashlib import md5

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

# only require staff access if this is the archive
@needs_login(staffer=True, archive_only=True)
def queuestatus(req, things):
    if len(things) == 1 and things[0] == 'json':
        return queuestatus_update(req, things)
    elif len(things) > 1:
        try:
            return queuestatus_tb(req, things[0], int(things[1]))
        except (TypeError, ValueError):
            # things[1] is not a valid integer, thus not an ID...
            pass
        except UnknownQueueError:
            # Something failed under queuestatus_tb...
            pass
    return queuestatus_summary(req)

def queuestatus_update(req, things):
    cache = {}

    ctx = Context()

    # Try to decode the payload in the POST query
    try:
        cache = ctx.req.json
        if type(cache) != dict:
            cache = {}
    except ValueError:
        pass

    with session_scope() as session:

        result = []
        for qstat in stats(session):
            queue, lqname, tsize, terr = qstat['type'], qstat['lname'], qstat['size'], qstat['errors']
            esummary = list(error_summary(session, queue, DETAIL_THRESHOLD))
            summary = list(regular_summary(session, queue, DETAIL_THRESHOLD))
            ids = [tsize, terr] + [x['oid'] for x in summary + esummary]
            dig = md5(str(ids)).hexdigest()
            if dig != cache.get(lqname):
                result.append(dict(queue=lqname, token=dig, waiting=summary, errors=esummary, total_waiting=tsize, total_errors=terr))
            else:
                result.append(dict(queue=lqname, token=dig))

        ctx.resp.content_type = 'application/json'
        ctx.resp.append_json(result)

    return apache.HTTP_OK
