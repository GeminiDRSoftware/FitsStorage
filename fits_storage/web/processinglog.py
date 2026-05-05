from sqlalchemy import select, join, or_

from fits_storage.server.orm.processinglog import ProcessingLog, ProcessingLogFile

from fits_storage.server.wsgi.context import get_context
from . import templating

@templating.templated("processinglog.html")
def processinglog(thing):

    processinglog_id = None
    filename = None
    search = False
    try:
        processinglog_id = int(thing)
    except ValueError:
        filename = thing
        if filename.startswith('search='):
            search = True
            filename = filename[7:]

    ctx = get_context()

    if processinglog_id:
        stmt = select(ProcessingLog).where(ProcessingLog.id == processinglog_id)
    elif search:
        stmt = (select(ProcessingLog)
                .where(or_(ProcessingLog.input_files.contains(filename),
                           ProcessingLog.output_files.contains(filename)))
                )
    else:
        stmt = (select(ProcessingLog)
                .select_from(join(ProcessingLog, ProcessingLogFile))
                .where(ProcessingLogFile.filename==filename))

    pls = ctx.session.execute(stmt).scalars().all()

    template_args = dict(
        pls = pls,
        num = len(pls)
    )

    return template_args
