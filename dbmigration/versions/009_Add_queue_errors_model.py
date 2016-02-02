from sqlalchemy import *
from migrate import *
from fits_storage.orm.queue_error import QueueError

def upgrade(migrate_engine):
    QueueError.metadata.create_all(migrate_engine)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    queue_error = Table('queue_error', meta, autoload=True)
    queue_error.drop()
