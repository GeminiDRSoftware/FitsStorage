from sqlalchemy import *
from migrate import *
from fits_storage.orm.publication import Publication
from fits_storage.orm.programpublication import ProgramPublication

def upgrade(migrate_engine):
    Publication.metadata.create_all(migrate_engine)
    ProgramPublication.metadata.create_all(migrate_engine)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    for name in ('publication', 'programpublication'):
        table = Table(name, meta, autoload=True)
        table.drop()
