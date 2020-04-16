from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    archiveuser = Table('archiveuser', meta, autoload=True)

    misc_upload    = Column('misc_upload', Boolean)

    misc_upload.create(archiveuser)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    archiveuser = Table('archiveuser', meta, autoload=True)

    archiveuser.c.misc_upload.drop()
