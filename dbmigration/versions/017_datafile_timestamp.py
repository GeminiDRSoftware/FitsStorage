from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    diskfile = Table('diskfile', meta, autoload=True)

    datafile_timestamp = Column('datafile_timestamp', DateTime(timezone=True))

    datafile_timestamp.create(diskfile)

    i = Index('idx_diskfile_datafile_timestamp', datafile_timestamp)
    i.create(migrate_engine)

    with migrate_engine.connect() as connection:
        connection.execute("update diskfile set datafile_timestamp=to_date(substring(filename from 2 for 8), 'YYYYMMDD') where filename like 'N20[0-9][0-9][0-1][0-9][0-3][0-9]%'")
        connection.execute("update diskfile set datafile_timestamp=to_date(substring(filename from 5 for 8), 'YYYYMMDD') where filename like 'img_20[0-9][0-9][0-1][0-9][0-3][0-9]%'")
        connection.execute("update diskfile set datafile_timestamp=to_date(substring(filename from 6 for 8), 'YYYYMMDD') where filename like '[A-Z][A-Z][A-Z][A-Z]_20[0-9][0-9][0-1][0-9][0-3][0-9]%'")
        connection.execute("update diskfile set datafile_timestamp=lastmod where datafile_timestamp is NULL")


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('diskfile', meta, autoload=True)

    user.c.datafile_timestamp.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=17
