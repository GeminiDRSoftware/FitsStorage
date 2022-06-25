from sqlalchemy import *
from migrate import *

from gemini_obs_db.orm.ghost import Ghost


def upgrade(migrate_engine):
    Ghost.metadata.create_all(bind=migrate_engine)


def downgrade(migrate_engine):
    Ghost.drop(bind=migrate_engine)


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=30
