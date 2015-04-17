from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    used_date = Column('used_date', Boolean)

    used_date.create(version)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    version.c.used_date.drop()
