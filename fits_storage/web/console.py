from . import templating
from ..orm.exportqueue import ExportQueue
from ..orm.ingestqueue import IngestQueue
from ..orm.queue_error import QueueError

from ..utils.web import get_context


__all__ = ["console", "console_ingest_queue", "console_ingest_errors", "console_export_queue"]


@templating.templated("console/console.html")
def console():
    ctx = get_context()

    session = ctx.session

    ingest_count = session.query(IngestQueue.id).count()
    export_count = session.query(ExportQueue.id).count()
    ingest_fail_count = session.query(IngestQueue).filter(IngestQueue.failed == True).count()
    export_fail_count = session.query(ExportQueue).filter(ExportQueue.failed == True).count()

    return dict(ingest_count=ingest_count, export_count=export_count, ingest_fail_count=ingest_fail_count,
                export_fail_count=export_fail_count)


@templating.templated("console/ingest_queue.html")
def console_ingest_queue():
    ctx = get_context()

    session = ctx.session

    formdata = ctx.get_form_data()
    page = 1
    num_per_page = 20
    if formdata:
        if 'page' in formdata:
            page = formdata['page'].value
            page = int(page)
    count = session.query(IngestQueue).count()
    num_pages = int((count-1)/num_per_page) + 1

    ingest_queue = session.query(IngestQueue).offset((page-1)*num_per_page).limit(num_per_page)

    return dict(ingest_queue=ingest_queue, page=page, num_pages=num_pages)


@templating.templated("console/export_queue.html")
def console_export_queue():
    ctx = get_context()

    session = ctx.session

    export_queue = session.query(ExportQueue)

    return dict(export_queue=export_queue)


@templating.templated("console/ingest_errors.html")
def console_ingest_errors():
    ctx = get_context()
    formdata = ctx.get_form_data()

    session = ctx.session

    ingest_errors = session.query(IngestQueue).filter(IngestQueue.failed == True).order_by(IngestQueue.added.desc())

    error_details = None
    det = None
    if formdata and 'detail' in formdata:
        det = formdata['detail'].value
        qes = session.query(QueueError).filter(QueueError.filename == det).filter(QueueError.queue == 'INGEST') \
            .order_by(QueueError.added.desc())
        error_details = qes
    return dict(ingest_errors=ingest_errors, error_details=error_details, detail=det)
