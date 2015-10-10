from sqlalchemy import *
from migrate import *
from fits_storage.orm.miscfile import MiscFile

def upgrade(migrate_engine):
    MiscFile.metadata.create_all(migrate_engine)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    miscfile = Table('miscfile', meta, autoload=True)
    miscfile.drop()
