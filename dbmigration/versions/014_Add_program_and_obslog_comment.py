from sqlalchemy import *
from migrate import *
from fits_storage.orm.program import Program
from fits_storage.orm.obslog_comment import ObslogComment

def upgrade(migrate_engine):
    Program.metadata.create_all(migrate_engine)
    ObslogComment.metadata.create_all(migrate_engine)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    for name in ('program', 'obslog_comment'):
        table = Table(name, meta, autoload=True)
        table.drop()
