from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    coadds    = Column('coadds', Integer)
    prop_coor = Column('proprietary_coordinates', Boolean)

    coadds.create(header)
    prop_coor.create(header)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    header.c.coadds.drop()
    header.c.proprietary_coordinates.drop()
