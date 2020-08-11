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
        for year in range(2020, 2000, -1):
            for month in range(12,1,-1):
                datestr = "%4d%2d" % (year, month)
                with connection.begin():
                    connection.execute(sqlalchemy.text("update diskfile set datafile_timestamp=to_date(substring(filename, 2, 8), 'YYYYMMDD') where filename like 'N{datestr}%'".format(datestr=datestr)))
                with connection.begin():
                    connection.execute(sqlalchemy.text("update diskfile set datafile_timestamp=to_date(substring(filename, 5, 8), 'YYYYMMDD') where filename like 'img_{datestr}%'".format(datestr=datestr)))
                with connection.begin():
                    connection.execute(sqlalchemy.text("update diskfile set datafile_timestamp=to_date(substring(filename, 6, 8), 'YYYYMMDD') where filename like '_____{datestr}%'".format(datestr=datestr)))
        with connection.begin():
            connection.execute(sqlalchemy.text("update diskfile set datafile_timestamp=lastmod where datafile_timestamp is NULL"))


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('diskfile', meta, autoload=True)

    user.c.datafile_timestamp.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=17
