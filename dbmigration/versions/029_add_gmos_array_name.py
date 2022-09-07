from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    array_name = Column('array_name', Text)

    array_name.create(gmos)

    i = Index('idx_gmos_array_name', array_name)
    i.create(migrate_engine)



def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos.c.array_name.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=29
