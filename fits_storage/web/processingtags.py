from sqlalchemy import select

from fits_storage.server.orm.processingtag import ProcessingTag

from fits_storage.server.wsgi.context import get_context
from . import templating

@templating.templated("processing_tags.html")
def processingtags():

    ctx = get_context()

    stmt = (select(ProcessingTag)
            .order_by(ProcessingTag.domain)
            .order_by(ProcessingTag.priority))

    tags = ctx.session.execute(stmt).scalars()

    template_args = dict(
        tags = tags
    )

    return template_args
