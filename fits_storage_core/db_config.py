from os.path import join

import os


__all__ = [
    "using_apache",
    "use_utc",
    "z_staging_area",
    "storage_root",
    "sqlite_db_path",
    "database_url",
    "postgres_database_pool_size",
    "postgres_database_max_overflow",
]


# Note: The application using this package may want to override
# these settings to get the database configuration appropriate
# for that use.
using_apache = False
use_utc = False
z_staging_area = ''
storage_root = os.getenv('STORAGE_ROOT', '')
sqlite_db_path = join(storage_root, 'gemini_obs_db.db')
database_url = os.getenv('GEMINI_OBS_DB_URL', 'sqlite:///' + sqlite_db_path)
database_debug = False  # set to True to enable SQLAlchemy debugging

# These two are only used if we are using a Postgres database
# However, we define them anyway so they are available for import
postgres_database_pool_size = 30
postgres_database_max_overflow = 10
