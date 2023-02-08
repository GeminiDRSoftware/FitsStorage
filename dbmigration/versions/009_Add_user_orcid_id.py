from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    orcid_id = Column('orcid_id', String(20))

    orcid_id.create(user)

    i = Index('idx_archiveuser_orcid_id', orcid_id)
    i.create(migrate_engine)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    user.c.orcid_id.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
