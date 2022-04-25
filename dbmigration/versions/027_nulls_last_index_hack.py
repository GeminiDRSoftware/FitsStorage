from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    with migrate_engine.connect() as connection:
        # HACK HACK HACK
        # Postgres does poorly with non-default nulls sorting on datetime so need to explicitly add the custom index
        connection.execute("CREATE INDEX ix_header_ut_datetime_nulls_last ON TABLE header (ut_datetime DESC NULLS LAST)")


def downgrade(migrate_engine):
    with migrate_engine.connect() as connection:
        connection.execute("DROP INDEX ix_header_ut_datetime_nulls_last")


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=27
