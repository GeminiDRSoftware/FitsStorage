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


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
