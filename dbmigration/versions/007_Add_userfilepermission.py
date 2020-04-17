from sqlalchemy import *
from migrate import *
from fits_storage.orm.userfilepermission import UserFilePermission


def upgrade(migrate_engine):
    UserFilePermission.metadata.create_all(migrate_engine)


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    userfilepermission = Table('userfilepermission', meta, autoload=True)
    userfilepermission.drop()
