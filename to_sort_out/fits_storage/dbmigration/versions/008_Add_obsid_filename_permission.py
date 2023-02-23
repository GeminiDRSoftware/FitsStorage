from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    userprogram = Table('userprogram', meta, autoload=True)

    observation_id    = Column('observation_id', Text)
    path    = Column('path', Text)
    filename    = Column('filename', Text)

    userprogram.c.program_id.alter(nullable=True)
    observation_id.create(userprogram)
    path.create(userprogram)
    filename.create(userprogram)


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    userprogram = Table('userprogram', meta, autoload=True)

    userprogram.c.observation_id.drop()
    userprogram.c.path.drop()
    userprogram.c.filename.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=10
