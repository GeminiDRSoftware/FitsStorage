from sqlalchemy import *
from migrate import *
from fits_storage.orm.miscfile import MiscFile

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    diskfile = Table('diskfile', meta, autoload=True)
    miscfile = Table('miscfile', meta,
            Column('id', Integer, primary_key=True),
            Column('diskfile_id', Integer, ForeignKey('diskfile.id'), nullable=False, index=True),
            Column('release', DateTime, nullable=False),
            Column('description', Text),
            Column('program_id', Text, index=True)
        )
    meta.create_all()

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    miscfile = Table('miscfile', meta, autoload=True)
    miscfile.drop()
