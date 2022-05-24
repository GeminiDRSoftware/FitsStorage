from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos_ara = Column('array_name', Text, index=True)

    gmos_ara.create(gmos, index_name='gmos_array_name_idx')


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos.c.array_name.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=29
