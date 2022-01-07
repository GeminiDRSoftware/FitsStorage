from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    exportqueue = Table('exportqueue', meta, autoload=True)

    sortkey = Column('sortkey', Text)

    sortkey.create(exportqueue)
    with migrate_engine.connect() as connection:
        # connection.execute("ALTER TYPE reduction_state ADD VALUE 'PROCESSED_UNKNOWN'")
        # connection.commit()

        # HACK HACK HACK
        # Can't use ALTER TYPE reduction_state ADD VALUE inside a transaction because postgres
        # So, we do this
        # see: https://stackoverflow.com/questions/1771543/adding-a-new-value-to-an-existing-enum-type/41696273#41696273
        connection.execute("UPDATE exportqueue set sortkey=filename")


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    exportqueue = Table('exportqueue', meta, autoload=True)

    exportqueue.c.sortkey.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=18
