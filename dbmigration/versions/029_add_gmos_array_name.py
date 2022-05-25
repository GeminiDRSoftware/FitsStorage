from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos_ara = Column('array_name', Text, index=True)

    gmos_ara.create(gmos, index_name='gmos_array_name_idx')
    with migrate_engine.connect() as connection:
        # breaking this up to not hammer the DB tlog too badly
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<100000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<200000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<300000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<400000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<500000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<600000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<700000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<800000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<900000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1000000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1100000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1200000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1300000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1400000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1500000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1600000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1700000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1800000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<1900000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<2000000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<2500000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL and id<300000")
        connection.execute("update gmos set array_name=REGEXP_REPLACE(REGEXP_REPLACE(amp_read_area, ':\[.+?\]', '', 'g'), '''', '', 'g') where array_name IS NULL")

def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    gmos = Table('gmos', meta, autoload=True)

    gmos.c.array_name.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=29
