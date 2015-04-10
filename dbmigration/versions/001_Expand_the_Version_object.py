from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    score = Column('score', Integer, default = -1)
    accepted = Column('accepted', Boolean)

    score.create(version)
    accepted.create(version)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    version = Table('versions', meta, autoload=True)
    version.c.accepted.drop()
    version.c.score.drop()
