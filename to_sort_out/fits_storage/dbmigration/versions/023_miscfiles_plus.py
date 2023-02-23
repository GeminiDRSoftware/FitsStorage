from sqlalchemy import *
from migrate import *

from fits_storage.orm.miscfile_plus import MiscFileCollection, MiscFileFolder, MiscFilePlus, MiscFileCollectionUsers
from fits_storage.orm.user import User


def upgrade(migrate_engine):
    MiscFileCollection.metadata.create_all(bind=migrate_engine)
    MiscFileFolder.metadata.create_all(bind=migrate_engine)
    MiscFilePlus.metadata.create_all(bind=migrate_engine)


def downgrade(migrate_engine):
    MiscFileCollectionUsers.drop(bind=migrate_engine)
    MiscFileCollection.metadata.drop(bind=migrate_engine)
    MiscFileFolder.metadata.drop(bind=migrate_engine)
    MiscFilePlus.metadata.drop(bind=migrate_engine)


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=23
