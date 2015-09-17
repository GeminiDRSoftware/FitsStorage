"""
This module contains the tape related xml generator functions. 
"""
from ..orm import session_scope
from ..orm.tapestuff import Tape, TapeWrite, TapeFile

from . import templating

from ..apache_return_codes import HTTP_OK

# We use these wrappers in order to trigger separate query for the
# internal objects. This is done only to conserve memory, because
# SQLAlchemy tends to hog a lot of it and we may end up being killed.
# Otherwise we could just use Tape.tapewrites and TapeWrite.tapefiles...
class QueryWrapper(object):
    def __init__(self, session, query, Wrapper):
        self.s = session
        self.q = query
        self.W = Wrapper

    def __iter__(self):
        for obj in self.q:
            yield self.W(self.s, obj)

class TapeWrapper(object):
    def __init__(self, session, obj):
        self.s = session
        self.o = obj

    def __getattr__(self, attr):
        return getattr(self.o, attr)

    @property
    def tapewrites(self):
        q = (self.s.query(TapeWrite)
                    .filter(TapeWrite.tape_id == self.o.id)
                    .filter(TapeWrite.suceeded == True)
                    .order_by(TapeWrite.id))
        return QueryWrapper(self.s, q, TapeWriterWrapper)

class TapeWriterWrapper(object):
    def __init__(self, session, obj):
        self.s = session
        self.o = obj

    def __getattr__(self, attr):
        return getattr(self.o, attr)

    @property
    def tapefiles(self):
        return self.s.query(TapeFile).filter(TapeFile.tapewrite_id == self.o.id)

def xmltape(req):
    """
    Outputs xml describing the tapes that the specified file is on
    """
    req.content_type = "text/xml"

    with session_scope() as session:
        template = templating.get_env().get_template('tape.xml')
        query = session.query(Tape).filter(Tape.active == True).order_by(Tape.id)
        # Potentially huge reponse. Better to chunk it
        for text in template.generate(tapes = QueryWrapper(session, query, TapeWrapper)):
            req.write(text)
    return HTTP_OK

