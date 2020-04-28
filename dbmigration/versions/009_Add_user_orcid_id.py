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
