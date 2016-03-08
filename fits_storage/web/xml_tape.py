"""
This module contains the tape related xml generator functions.
"""
from ..orm.tapestuff import Tape, TapeWrite, TapeFile

from . import templating

from ..utils.web.adapter import get_context

# We use these wrappers in order to trigger separate query for the
# internal objects. This is done only to conserve memory, because
# SQLAlchemy tends to hog a lot of it and we may end up being killed.
# Otherwise we could just use Tape.tapewrites and TapeWrite.tapefiles...
class QueryWrapper(object):
    def __init__(self, query, Wrapper):
        self.q = query
        self.W = Wrapper

    def __iter__(self):
        for obj in self.q:
            yield self.W(obj)

class TapeWrapper(object):
    def __init__(self, obj):
        self.o = obj

    def __getattr__(self, attr):
        return getattr(self.o, attr)

    @property
    def tapewrites(self):
        q = (get_context().session.query(TapeWrite)
                    .filter(TapeWrite.tape_id == self.o.id)
                    .filter(TapeWrite.suceeded == True)
                    .order_by(TapeWrite.id))
        return QueryWrapper(q, TapeWriterWrapper)

class TapeWriterWrapper(object):
    def __init__(self, obj):
        self.o = obj

    def __getattr__(self, attr):
        return getattr(self.o, attr)

    @property
    def tapefiles(self):
        return get_context().session.query(TapeFile).filter(TapeFile.tapewrite_id == self.o.id)

@templating.templated("tape.xml", content_type='text/xml', with_generator=True)
def xmltape():
    """
    Outputs xml describing the tapes that the specified file is on
    """

    query = get_context().session.query(Tape).filter(Tape.active == True).order_by(Tape.id)

    return dict(
        tapes = QueryWrapper(query, TapeWrapper)
        )
