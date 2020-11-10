from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    previewqueue = Table('previewqueue', meta, autoload=True)

    force    = Column('force', Boolean)

    force.create(previewqueue)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    previewqueue = Table('previewqueue', meta, autoload=True)

    previewqueue.c.force.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=19
