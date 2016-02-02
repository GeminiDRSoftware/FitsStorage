from sqlalchemy import *
from migrate import *
from migrate.changeset.constraint import UniqueConstraint

table_constr = (
    ('calcachequeue', ('obs_hid',)),
    ('previewqueue', ('diskfile_id',)),
    ('ingestqueue', ('filename', 'inprogress')),
    ('exportqueue', ('filename', 'inprogress'))
)

def get_constraints(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    for table, columns in table_constr:
        yield UniqueConstraint(*columns, table=Table(table, meta, autoload=True))

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for constr in get_constraints(migrate_engine):
        constr.create()

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)

    for constr in get_constraints(migrate_engine):
        constr.drop()
