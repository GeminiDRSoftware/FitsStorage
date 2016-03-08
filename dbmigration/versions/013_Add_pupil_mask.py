from sqlalchemy import *
from migrate import *

tables = 'header', 'gpi'

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for table in (Table(t, meta, autoload=True) for t in tables):
        pupil_mask = Column('pupil_mask', Text)
        pupil_mask.create(table, index_name='ix_{}_pupil_mask'.format(table.name))

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for table in (Table(t, meta, autoload=True) for t in tables):
        table.c.pupil_mask.drop()
