from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fits_storage.orm.createtables import create_tables

print("test")

# Do this first so we can set the DB to sqlite
from fits_storage import fits_storage_config as fsc

fsc.fits_database = 'sqlite:////tmp/sqlite-database'

#--

from datetime import datetime, timedelta

from fits_storage.fits_storage_config import delete_min_days_age
from fits_storage.orm.exportqueue import ExportQueue
from fits_storage.scripts.delete_files import check_old_enough_to_delete, check_not_on_export_queue, add_to_msg, \
    add_to_errmsg
from fits_storage.scripts import delete_files

from gemini_obs_db.db import session_scope


def test_add_to_msg():
    logtext = None
    delete_files.msglines = []

    def log_fn(msg):
        nonlocal logtext
        logtext = msg
    add_to_msg(log_fn, 'testing')
    assert(delete_files.msglines == ['testing'])
    assert(logtext == 'testing')


def test_add_to_errmsg():
    delete_files.errmsglines = None
    logtext = None
    def log_fn(msg):
        nonlocal logtext
        logtext = msg
    add_to_errmsg(log_fn, 'testing')
    assert(delete_files.errmsglines is not None)
    assert(len(delete_files.errmsglines) == 1)
    assert(delete_files.errmsglines[0] == 'testing')
    assert(logtext == 'testing')
    add_to_errmsg(log_fn, 'testing2')
    assert(len(delete_files.errmsglines) == 2)
    assert(delete_files.errmsglines[0] == 'testing')
    assert(delete_files.errmsglines[1] == 'testing2')
    assert(logtext == 'testing2')


def test_old_enough():
    if delete_min_days_age:
        dt = datetime.now() - timedelta(days=(delete_min_days_age+5))
    filename = 'N%sX0000.fits' % dt.strftime('%Y%m%d')
    assert(check_old_enough_to_delete(filename))
    if delete_min_days_age:
        dt = datetime.now() - timedelta(days=(delete_min_days_age-5))
    filename = 'N%sX0000.fits' % dt.strftime('%Y%m%d')
    assert(not check_old_enough_to_delete(filename))


def test_export_queue():
    pg_db = create_engine('sqlite://', echo=False)
    sessionfactory = sessionmaker(pg_db)

    session = sessionfactory()
    try:
        ExportQueue.metadata.create_all(bind=pg_db)
        filename = "test.fits"
        assert(check_not_on_export_queue(session, filename))
        eq = ExportQueue(filename="test.fits", path="", destination="")
        session.add(eq)
        assert(check_not_on_export_queue(session, filename) == False)
    except Exception as e:
        session.close()
        raise
    session.close()
