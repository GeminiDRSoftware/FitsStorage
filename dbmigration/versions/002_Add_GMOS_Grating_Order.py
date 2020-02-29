from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    grating_order    = Column('grating_order', Integer)

    grating_order.create(gmos)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos.c.grating_order.drop()
