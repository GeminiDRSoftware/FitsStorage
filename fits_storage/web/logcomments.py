"""
This module deals with displaying information about programs.
"""

from . import templating

from fits_storage.core.orm.header import Header
from fits_storage.server.orm.obslog_comment import ObslogComment
from fits_storage.server.wsgi.context import get_context


@templating.templated("logcomments.html")
def log_comments(selection):
    session = get_context().session
    query = session.query(Header.data_label, Header.ut_datetime,
                          ObslogComment.comment)
    query = query.select_from(Header)\
        .join(ObslogComment, Header.data_label == ObslogComment.data_label)
    query = selection.filter(query).order_by(Header.data_label)

    def generate_dict(it):
        for row in it:
            yield {'datalabel': row[0], 'ut_datetime': row[1],
                   'comment': row[2]}

    return {'rows': generate_dict(query)}
