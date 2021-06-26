from sqlalchemy import *
from migrate import *
from gemini_obs_db.diskfile import DiskFile
from fits_storage.orm.provenance import Provenance, ProvenanceHistory

def upgrade(migrate_engine):
    Provenance.metadata.create_all(migrate_engine)
    ProvenanceHistory.metadata.create_all(migrate_engine)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    provenance = Table('provenance', meta, autoload=True)
    provenance.drop()
    provenance_history = Table('provenance_history', meta, autoload=True)
    provenance_history.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
