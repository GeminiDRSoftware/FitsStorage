from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    calcachequeue = Table('calcachequeue', meta, autoload=True)

    filename = Column('filename', Text)

    filename.create(calcachequeue)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('calcachequeue', meta, autoload=True)

    header.c.filename.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=18
