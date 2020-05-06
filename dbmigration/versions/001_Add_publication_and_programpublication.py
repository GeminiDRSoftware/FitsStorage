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


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
