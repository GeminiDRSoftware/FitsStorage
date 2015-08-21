from ..orm.ingestqueue import IngestQueue
from ..orm.calcachequeue import CalCacheQueue
from ..orm.previewqueue import PreviewQueue
from ..orm.exportqueue import ExportQueue
from ..orm.diskfile import DiskFile
from ..orm.header import Header

from .ingestqueue import IngestQueueUtil
from .calcachequeue import CalCacheQueueUtil
from .previewqueue import PreviewQueueUtil
from .exportqueue import ExportQueueUtil

from sqlalchemy import desc

queues = (
    ('Ingest', IngestQueue, IngestQueueUtil),
    ('Calibration Cache', CalCacheQueue, CalCacheQueueUtil),
    ('Previews', PreviewQueue, PreviewQueueUtil),
    ('Export', ExportQueue, ExportQueueUtil),
    )

def stats(session):
    for qname, qtype, qutil in queues:
        length = qutil(session, None).length()
        errors = (
            session.query(qtype)
                    .filter(qtype.inprogress==True)
                    .filter(qtype.error != None)
                .count()
            )
        yield {'name': qname,
               'type': qtype,
               'length': length,
               'errors': errors}

error_query = {
    # Type: (select_from, join, columns, sorting)
    IngestQueue:  (None, None, (IngestQueue.filename, IngestQueue.added), (IngestQueue.added,)),
    ExportQueue:  (None, None, (ExportQueue.filename, ExportQueue.added), (ExportQueue.added,)),
    PreviewQueue: ((DiskFile,), (PreviewQueue,), (DiskFile.filename,), (desc(DiskFile.filename),)),
    CalCacheQueue: ((DiskFile,), (Header, CalCacheQueue.obs_hid == Header.id,),
                    (DiskFile.filename,), (CalCacheQueue.ut_datetime,)),
    }

def error_summary(session, qtype, lim):
    sel_from, join, columns, sort = error_query[qtype]
    q = session.query(*columns)
    if sel_from is not None:
        q = q.select_from(*sel_from)
        if join is not None:
            q = q.join(*join)
    if sort is not None:
        q = q.order_by(*sort)
    for res in q.filter(qtype.inprogress==True).filter(qtype.error != None).limit(lim):
        yield {'filename': res[0],
               'since':    (res[1].strftime('%Y-%m-%d %H:%M') if len(res) == 2 else 'Unknown')}
