from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    calcachequeue = Table('userprogram', meta, autoload=True)

    filename = Column('path', Text, index_name='userprogram_path_idx')

    filename.create(calcachequeue)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('userprogram', meta, autoload=True)

    header.c.path.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=19
