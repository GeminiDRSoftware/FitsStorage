from sqlalchemy import *
from migrate import *

from gemini_obs_db.header import PROCSCI_ENUM


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    PROCSCI_ENUM.create(migrate_engine)
    if hasattr(header.c, 'procsci'):
        header.c.procsci.drop()
    procsci = Column('procsci', PROCSCI_ENUM)

    procsci.create(header)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    if hasattr(header.c, 'procsci'):
        header.c.procsci.drop()
    PROCSCI_ENUM.drop(migrate_engine)

    # recreate varchar col
    procsci    = Column('procsci', String(4))

    procsci.create(header)


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
