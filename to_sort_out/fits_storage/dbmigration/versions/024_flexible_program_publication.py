from sqlalchemy import *


def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    programpublication = Table('programpublication', meta, autoload=True)
    programpublication.c.prog_id.alter(nullable=True)


def downgrade(migrate_engine):
    pass


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=24
