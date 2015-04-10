from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    is_clear = Column('is_clear', Boolean)

    is_clear.create(version)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    version.c.is_clear.drop()
