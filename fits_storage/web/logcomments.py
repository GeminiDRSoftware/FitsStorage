"""
This module deals with displaying information about programs.
"""

from . import templating
from .summary import list_headers
from .selection import queryselection

from ..orm.header import Header
from ..orm.obslog_comment import ObslogComment
from ..utils.web import get_context

@templating.templated("logcomments.html")
def log_comments(selection):
    session = get_context().session
    query = session.query(Header.data_label, Header.ut_datetime, ObslogComment.comment)
    query = query.select_from(Header).join(ObslogComment, Header.data_label == ObslogComment.data_label)
    query = queryselection(query, selection).order_by(Header.data_label)

    def generate_dict(it):
        for row in it:
            yield {'datalabel': row[0], 'ut_datetime': row[1], 'comment': row[2]}

    return { 'rows': generate_dict(query) }
