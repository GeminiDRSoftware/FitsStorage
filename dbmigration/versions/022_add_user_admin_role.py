from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    user_admin = Column('user_admin', Boolean)

    user_admin.create(user)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    user = Table('archiveuser', meta, autoload=True)

    user.c.user_admin.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=22
