from sqlalchemy import *
from migrate import *
from orm.diskfilereport import STATUS_ENUM

def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    diskfile = Table('diskfile', meta, autoload=True)
    diskfilereport = Table('diskfilereport', meta, autoload=True)
    mdstatus = Column('mdstatus', STATUS_ENUM)

    STATUS_ENUM.create(bind = migrate_engine)
    diskfile.c.wmdready.alter(name='mdready')
    diskfilereport.c.wmdreport.alter(name='mdreport')
    mdstatus.create(diskfilereport, index_name='ix_diskfilereport_mdstatus')

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    diskfile = Table('diskfile', meta, autoload=True)
    diskfilereport = Table('diskfilereport', meta, autoload=True)

    diskfile.c.mdready.alter(name='wmdready')
    diskfilereport.c.mdreport.alter(name='wmdreport')
    diskfilereport.c.mdstatus.drop()
    STATUS_ENUM.drop(bind = migrate_engine)
