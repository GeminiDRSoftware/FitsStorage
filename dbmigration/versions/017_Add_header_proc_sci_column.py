from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    procsci    = Column('procsci', String(4))

    procsci.create(header)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    header.c.procsci.drop()
