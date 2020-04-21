from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    with migrate_engine.connect() as connection:
        result = connection.execute( "ALTER TYPE obstype ADD VALUE 'STANDARD'")
        connection.commit()

def downgrade(migrate_engine):
    pass
