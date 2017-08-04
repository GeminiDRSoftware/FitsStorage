"""
This module handles the web 'queue' functions. Mainly showing status
"""

from ..utils.queuestats import stats, regular_summary, error_summary, error_detail, UnknownQueueError
from ..utils.web import get_context

from .user import needs_login

from ..orm.ingestqueue import IngestQueue
from ..orm.exportqueue import ExportQueue
from ..orm.previewqueue import PreviewQueue
from ..orm.calcachequeue import CalCacheQueue

from ..fits_storage_config import fits_servertitle, fits_servername

from . import templating

import json
from hashlib import md5

DETAIL_THRESHOLD = 20

@needs_login(staffer=True, archive_only=True)
@templating.templated("queuestatus/index.html")
def queuestatus_summary():
    general_rows = []
    detail_tables = {}

    session = get_context().session
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
        servertitle = fits_servertitle,
        servername = fits_servername,
        general_rows  = general_rows,
        detail_tables = detail_tables
        )

    return template_args

@needs_login(staffer=True, archive_only=True)
@templating.templated("queuestatus/detail.html")
def queuestatus_tb(qshortname, oid):
    det = error_detail(get_context().session, qshortname, oid)

    template_args = dict(
        servertitle = fits_servertitle,
        servername = fits_servername,
        oid = oid,
        **det
        )

    return template_args

QUEUELIMIT = 200

#@needs_login(staffer=True, archive_only=True)
#def queuestatus(things):
#    if len(things) == 1 and things[0] == 'json':
#        return queuestatus_update(things)
#    elif len(things) > 1:
#        try:
#            return queuestatus_tb(things[0], int(things[1]))
#        except (TypeError, ValueError):
#            # things[1] is not a valid integer, thus not an ID...
#            pass
#        except UnknownQueueError:
#            # Something failed under queuestatus_tb...
#            pass
#    return queuestatus_summary()

@needs_login(staffer=True, archive_only=True)
def queuestatus_update():
    cache = {}

    ctx = get_context()

    # Try to decode the payload in the POST query
    try:
        cache = ctx.json
        if type(cache) != dict:
            cache = {}
    except ValueError:
        pass

    session = get_context().session

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
