from ..orm.ingestqueue import IngestQueue
from ..orm.calcachequeue import CalCacheQueue
from ..orm.previewqueue import PreviewQueue
from ..orm.exportqueue import ExportQueue
from ..orm.queue_error import QueueError
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from .ingestqueue import IngestQueueUtil
from .calcachequeue import CalCacheQueueUtil
from .previewqueue import PreviewQueueUtil
from .exportqueue import ExportQueueUtil

from sqlalchemy import desc

from collections import namedtuple

queues = (
    ('Ingest', 'iq', IngestQueue, IngestQueueUtil),
    ('Export', 'eq', ExportQueue, ExportQueueUtil),
    ('Preview', 'pq', PreviewQueue, PreviewQueueUtil),
    ('Calibration Cache', 'cq', CalCacheQueue, CalCacheQueueUtil),
    )

def stats(session):
    for qname, linkname, qtype, qutil in queues:
        length = qutil(session, None).length()
        errors = (
            session.query(QueueError)
                    .filter(QueueError.queue == qtype.error_name)
                .count()
            )
        yield {'name': qname,
               'lname': linkname,
               'type': qtype,
               'size': length,
               'errors': errors}

stat_query = {
    # Type: (select_from, join, columns, sorting)
    IngestQueue:  (None, None,
                   (IngestQueue.id, IngestQueue.filename, IngestQueue.added),
                   (IngestQueue.added,)),
    ExportQueue:  (None, None,
                   (ExportQueue.id, ExportQueue.filename, ExportQueue.added),
                   (ExportQueue.added,)),
    PreviewQueue: ((DiskFile,), (PreviewQueue,),
                   (PreviewQueue.id, DiskFile.filename),
                   (desc(DiskFile.filename),)),
    CalCacheQueue: ((DiskFile,), (Header, (CalCacheQueue, CalCacheQueue.obs_hid == Header.id)),
                    (CalCacheQueue.id, DiskFile.filename, CalCacheQueue.ut_datetime),
                    (CalCacheQueue.ut_datetime,)),
    }

StatResult = namedtuple('StatResult', 'oid filename error added')

def get_error_result(session, obj=None, oid=None):
    if obj is None:
        obj = session.query(QueueError).get(oid)
    else:
        oid = obj.id
    return StatResult(oid=oid, filename=obj.filename, error=obj.error,
                       added=(obj.added.strftime('%Y-%m-%d %H:%M') if obj.added else 'Unknown'))


def compose_stat_query(session, qtype, *filters):
    sel_from, join, columns, sort = stat_query[qtype]
    q = session.query(*columns)
    if sel_from is not None:
        q = q.select_from(*sel_from)
        if join is not None:
            for j in join:
                if isinstance(j, tuple):
                    q = q.join(*j)
                else:
                    q = q.join(j)
    if sort is not None:
        q = q.order_by(*sort)

    for f in filters:
        q = q.filter(f)

    return q

def error_summary(session, qtype, lim):
    q = QueueError.get_errors_from(session, qtype)
    for res in (get_error_result(session, obj=row) for row in q.limit(lim)):
        yield {'oid':      res.oid,
               'filename': res.filename,
               'since':    res.added}

def regular_summary(session, qtype, lim):
    q = compose_stat_query(session, qtype, qtype.failed == False)
    for row in q.limit(lim):
        since = row[2].strftime('%Y-%m-%d %H:%M') if len(row)==3 else 'Unknown'
        yield {'oid':      row[0],
               'filename': row[1],
               'since':    since}

class UnknownQueueError(Exception):
    pass

def error_detail(session, lname, oid):
    for qname, linkname, qtype, qutil in queues:
        if linkname == lname:
            res = get_error_result(session, oid=oid)
            return {'qname':    qname,
                    'filename': res.filename,
                    'since':    res.added,
                    'tb':       res.error}
    raise UnknownQueueError("'{lname}' is not the associated name for any known queue".format(lname))
