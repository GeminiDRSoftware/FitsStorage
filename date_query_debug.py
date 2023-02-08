from fits_storage import fits_storage_config
from gemini_obs_db import db_config
from gemini_obs_db.utils.gemini_metadata_utils import get_time_period, get_date_offset

print("Use As Archive: %s" % fits_storage_config.use_as_archive)
print("Use UTC: %s" % db_config.use_utc)

print("Checking get_time_period(20130711)")
print("Got: %s %s" % get_time_period("20130711"))

print("Checking get_date_offset()")
print("Got: %s" % get_date_offset())

