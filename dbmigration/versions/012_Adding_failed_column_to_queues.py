from sqlalchemy import *
from migrate import *

def get_queues(meta):
    for t in ('calcachequeue', 'exportqueue', 'ingestqueue', 'previewqueue'):
        yield Table(t, meta, autoload=True)

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for queue in get_queues(meta):
        failed = Column('failed', Boolean)
        failed.create(queue)
    print "Done! Remeber to set all the queues.failed columns to False, if there are values"

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for queue in get_queues(meta):
        queue.c.failed.drop()
