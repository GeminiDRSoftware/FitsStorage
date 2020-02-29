from sqlalchemy import *
from migrate import *
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
