from datetime import datetime, timedelta

from fits_storage.fits_storage_config import delete_min_days_age
from fits_storage.scripts.delete_files import check_old_enough_to_delete


def test_old_enough():
    if delete_min_days_age:
        dt = datetime.now() - timedelta(days=(delete_min_days_age+5))
    filename = 'N%sX0000.fits' % dt.strftime('%Y%m%d')
    assert(check_old_enough_to_delete(filename))
    if delete_min_days_age:
        dt = datetime.now() - timedelta(days=(delete_min_days_age-5))
    filename = 'N%sX0000.fits' % dt.strftime('%Y%m%d')
    assert(not check_old_enough_to_delete(filename))
