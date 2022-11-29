from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    ghost = Table('ghost', meta, autoload=True)

    detector_name_blue = Column(Text, index=True)
    detector_name_red = Column(Text, index=True)
    detector_name_slitv = Column(Text, index=True)
    detector_x_bin_blue = Column(Integer, index=True)
    detector_x_bin_red = Column(Integer, index=True)
    detector_x_bin_slitv = Column(Integer, index=True)
    detector_y_bin_blue = Column(Integer, index=True)
    detector_y_bin_red = Column(Integer, index=True)
    detector_y_bin_slitv = Column(Integer, index=True)
    exposure_time_blue = Column(Numeric(precision=8, scale=4))
    exposure_time_red = Column(Numeric(precision=8, scale=4))
    exposure_time_slitv = Column(Numeric(precision=8, scale=4))
    gain_setting_blue = Column(GAIN_SETTING_ENUM, index=True)
    gain_setting_red = Column(GAIN_SETTING_ENUM, index=True)
    gain_setting_slitv = Column(GAIN_SETTING_ENUM, index=True)
    read_speed_setting_blue = Column(READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_red = Column(READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_slitv = Column(READ_SPEED_SETTING_ENUM, index=True)
    amp_read_area_blue = Column(Text, index=True)
    amp_read_area_red = Column(Text, index=True)
    amp_read_area_slitv = Column(Text, index=True)

    detector_name_blue.create(ghost)
    detector_name_red.create(ghost)
    detector_name_slitv.create(ghost)
    detector_x_bin_blue.create(ghost)
    detector_x_bin_red.create(ghost)
    detector_x_bin_slitv.create(ghost)
    detector_y_bin_blue.create(ghost)
    detector_y_bin_red.create(ghost)
    detector_y_bin_slitv.create(ghost)
    exposure_time_blue.create(ghost)
    exposure_time_red.create(ghost)
    exposure_time_slitv.create(ghost)
    gain_setting_blue.create(ghost)
    gain_setting_red.create(ghost)
    gain_setting_slitv.create(ghost)
    read_speed_setting_blue.create(ghost)
    read_speed_setting_red.create(ghost)
    read_speed_setting_slitv.create(ghost)
    amp_read_area_blue.create(ghost)
    amp_read_area_red.create(ghost)
    amp_read_area_slitv.create(ghost)


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    ghost = Table('ghost', meta, autoload=True)

    ghost.c.detector_name_blue.drop()
    ghost.c.detector_name_red.drop()
    ghost.c.detector_name_slitv.drop()
    ghost.c.detector_x_bin_blue.drop()
    ghost.c.detector_x_bin_red.drop()
    ghost.c.detector_x_bin_slitv.drop()
    ghost.c.detector_y_bin_blue.drop()
    ghost.c.detector_y_bin_red.drop()
    ghost.c.detector_y_bin_slitv.drop()
    ghost.c.exposure_time_blue.drop()
    ghost.c.exposure_time_red.drop()
    ghost.c.exposure_time_slitv.drop()
    ghost.c.gain_setting_blue.drop()
    ghost.c.gain_setting_red.drop()
    ghost.c.gain_setting_slitv.drop()
    ghost.c.read_speed_setting_blue.drop()
    ghost.c.read_speed_setting_red.drop()
    ghost.c.read_speed_setting_slitv.drop()
    ghost.c.amp_read_area_blue.drop()
    ghost.c.amp_read_area_red.drop()
    ghost.c.amp_read_area_slitv.drop()


# putting this here since most new migrations begin as a copy/paste
# DON'T FORGET TO UPDATE ANSIBLE archive_install.yml TO SET VERISON=x ON FRESH DB INSTALL
# i.e.:
#              query: update migrate_version set version=32
