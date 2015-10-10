from sqlalchemy import *
from migrate import *
from fits_storage.orm.glacier import Glacier

def upgrade(migrate_engine):
    Glacier.metadata.create_all(migrate_engine)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    glacier = Table('glacier', meta, autoload=True)
    glacier.drop()
