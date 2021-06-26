from sqlalchemy import *
from migrate import *


# Need for migration logic, since it's removed from codebase
from gemini_obs_db.header import PROCMODE_ENUM

procsci_codes = ('sq', 'ql', 'qa')
PROCSCI_ENUM = Enum(*procsci_codes, name='procsci')


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    PROCMODE_ENUM.create(migrate_engine)
    procmode    = Column('procmode', PROCMODE_ENUM)

    procmode.create(header)

    header.c.procsci.drop()
    PROCSCI_ENUM.drop(migrate_engine)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    header = Table('header', meta, autoload=True)

    # revert to PROCSCI column
    PROCSCI_ENUM.create(migrate_engine)
    procsci    = Column('procsci', PROCSCI_ENUM)

    procsci.create(header)

    header.c.procmode.drop()
    PROCMODE_ENUM.drop(migrate_engine)


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=21
