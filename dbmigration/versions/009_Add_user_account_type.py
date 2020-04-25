from sqlalchemy import *
from migrate import *

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    account_type    = Column('account_type', Text)

    account_type.create(user)

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    user.c.account_type.drop()
