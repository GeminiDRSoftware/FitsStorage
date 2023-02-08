from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    with migrate_engine.connect() as connection:
        # Adding the normal slitillum caltype as well

        # connection.execute("ALTER TYPE obstype ADD VALUE 'STANDARD'")
        # connection.commit()

        # HACK HACK HACK
        # Can't use ALTER TYPE obstype ADD VALUE inside a transaction because postgres
        # So, we do this
        # see: https://stackoverflow.com/questions/1771543/adding-a-new-value-to-an-existing-enum-type/41696273#41696273
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'caltype'::regtype::oid, 'slitillum', "
                           " MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'caltype'::regtype")


def downgrade(migrate_engine):
    pass


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
