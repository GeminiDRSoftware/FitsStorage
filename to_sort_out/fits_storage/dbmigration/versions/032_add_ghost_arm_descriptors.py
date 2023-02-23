from sqlalchemy import *
from migrate import *

from gemini_obs_db.orm.ghost import GAIN_SETTING_ENUM, READ_SPEED_SETTING_ENUM


def upgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    ghost = Table('ghost', meta, autoload=True)

    arm = Column('arm', Text, index=True)
    want_before_arc = Column('want_before_arc', Boolean)

    detector_name_blue = Column('detector_name_blue', Text, index=True)
    detector_name_red = Column('detector_name_red', Text, index=True)
    detector_name_slitv = Column('detector_name_slitv', Text, index=True)
    detector_x_bin_blue = Column('detector_x_bin_blue', Integer, index=True)
    detector_x_bin_red = Column('detector_x_bin_red', Integer, index=True)
    detector_x_bin_slitv = Column('detector_x_bin_slitv', Integer, index=True)
    detector_y_bin_blue = Column('detector_y_bin_blue', Integer, index=True)
    detector_y_bin_red = Column('detector_y_bin_red', Integer, index=True)
    detector_y_bin_slitv = Column('detector_y_bin_slitv', Integer, index=True)
    exposure_time_blue = Column('exposure_time_blue', Numeric(precision=8, scale=4))
    exposure_time_red = Column('exposure_time_red', Numeric(precision=8, scale=4))
    exposure_time_slitv = Column('exposure_time_slitv', Numeric(precision=8, scale=4))
    gain_setting_blue = Column('gain_setting_blue', GAIN_SETTING_ENUM, index=True)
    gain_setting_red = Column('gain_setting_red', GAIN_SETTING_ENUM, index=True)
    gain_setting_slitv = Column('gain_setting_slitv', GAIN_SETTING_ENUM, index=True)
    read_speed_setting_blue = Column('read_speed_setting_blue', READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_red = Column('read_speed_setting_red', READ_SPEED_SETTING_ENUM, index=True)
    read_speed_setting_slitv = Column('read_speed_setting_slitv', READ_SPEED_SETTING_ENUM, index=True)
    amp_read_area_blue = Column('amp_read_area_blue', Text, index=True)
    amp_read_area_red = Column('amp_read_area_red', Text, index=True)
    amp_read_area_slitv = Column('amp_read_area_slitv', Text, index=True)

    # not sure we need 'arm', remove here and above if not
    arm.create(ghost, index_name='idx_ghost_arm')
    want_before_arc.create(ghost)
    detector_name_blue.create(ghost, index_name="idx_ghost_detector_name_blue")
    detector_name_red.create(ghost, index_name="idx_ghost_detector_name_red")
    detector_name_slitv.create(ghost, index_name="idx_ghost_detector_name_slitv")
    detector_x_bin_blue.create(ghost, index_name="idx_ghost_detector_x_bin_blue")
    detector_x_bin_red.create(ghost, index_name="idx_ghost_detector_x_bin_red")
    detector_x_bin_slitv.create(ghost, index_name="idx_ghost_detector_x_bin_slitv")
    detector_y_bin_blue.create(ghost, index_name="idx_ghost_detector_y_bin_blue")
    detector_y_bin_red.create(ghost, index_name="idx_ghost_detector_y_bin_red")
    detector_y_bin_slitv.create(ghost, index_name="idx_ghost_detector_y_bin_slitv")
    exposure_time_blue.create(ghost, index_name="idx_ghost_exposure_time_blue")
    exposure_time_red.create(ghost, index_name="idx_ghost_exposure_time_red")
    exposure_time_slitv.create(ghost, index_name="idx_ghost_exposure_time_slitv")
    gain_setting_blue.create(ghost, index_name="idx_ghost_gain_setting_blue")
    gain_setting_red.create(ghost, index_name="idx_ghost_gain_setting_red")
    gain_setting_slitv.create(ghost, index_name="idx_ghost_gain_setting_slitv")
    read_speed_setting_blue.create(ghost, index_name="idx_ghost_read_speed_setting_blue")
    read_speed_setting_red.create(ghost, index_name="idx_ghost_read_speed_setting_red")
    read_speed_setting_slitv.create(ghost, index_name="idx_ghost_read_speed_setting_slitv")
    amp_read_area_blue.create(ghost, index_name="idx_ghost_amp_read_area_blue")
    amp_read_area_red.create(ghost, index_name="idx_ghost_amp_read_area_red")
    amp_read_area_slitv.create(ghost, index_name="idx_ghost_amp_read_area_slitv")

    with migrate_engine.connect() as connection:
        # connection.execute("ALTER TYPE ghost_read_speed_setting ADD VALUE 'medium'")
        # connection.execute("ALTER TYPE ghost_read_speed_setting ADD VALUE 'standard'")
        # connection.execute("ALTER TYPE ghost_read_speed_setting ADD VALUE 'unknown'")
        # connection.execute("ALTER TYPE detector_readspeed_setting ADD VALUE 'medium'")
        # connection.execute("ALTER TYPE detector_readspeed_setting ADD VALUE 'standard'")
        # connection.execute("ALTER TYPE detector_readspeed_setting ADD VALUE 'unknown'")
        # connection.execute("ALTER TYPE ghost_gain_setting ADD VALUE 'standard'")
        # connection.execute("ALTER TYPE detector_gain_setting ADD VALUE 'standard'")
        # connection.commit()

        # HACK HACK HACK
        # Can't use ALTER TYPE obstype ADD VALUE inside a transaction because postgres
        # So, we do this
        # see: https://stackoverflow.com/questions/1771543/adding-a-new-value-to-an-existing-enum-type/41696273#41696273

        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'ghost_read_speed_setting'::regtype::oid, 'medium', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'ghost_read_speed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'ghost_read_speed_setting'::regtype::oid, 'standard', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'ghost_read_speed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'ghost_read_speed_setting'::regtype::oid, 'unknown', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'ghost_read_speed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'detector_readspeed_setting'::regtype::oid, 'medium', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'detector_readspeed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'detector_readspeed_setting'::regtype::oid, 'standard', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'detector_readspeed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'detector_readspeed_setting'::regtype::oid, 'unknown', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'detector_readspeed_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'ghost_gain_setting'::regtype::oid, 'standard', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'ghost_gain_setting'::regtype )")
        connection.execute("INSERT INTO pg_enum (enumtypid, enumlabel, enumsortorder) "
                           "SELECT 'detector_gain_setting'::regtype::oid, 'standard', "
                           "( SELECT MAX(enumsortorder) + 1 FROM pg_enum "
                           "WHERE enumtypid = 'detector_gain_setting'::regtype )")


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    ghost = Table('ghost', meta, autoload=True)

    ghost.c.arm.drop()
    ghost.c.want_before_arc.drop()
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
