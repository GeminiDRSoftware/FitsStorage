from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    ingestq = Table('ingestqueue', meta, autoload=True)

    error = Column('error', Text)
    error.create(ingestq)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    ingestq = Table('ingestqueue', meta, autoload=True)

    ingestq.c.error.drop()
