from sqlalchemy import *
from migrate import *

def get_queues(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    ccq = Table('calcachequeue', meta, autoload=True)
    exportq = Table('exportqueue', meta, autoload=True)
    ingestq = Table('ingestqueue', meta, autoload=True)
    previewq = Table('previewqueue', meta, autoload=True)

    return ccq, exportq, ingestq, previewq

def upgrade(migrate_engine):
    for queue in get_queues(migrate_engine):
        print "Creating for queue:", queue
        error = Column('error', Text)
        error.create(queue)

def downgrade(migrate_engine):
    for queue in get_queues(migrate_engine):
        queue.c.error.drop()
