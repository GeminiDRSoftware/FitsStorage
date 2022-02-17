__version__ = '2022-1.6'


from fits_storage import fits_storage_config
from gemini_obs_db import db_config

# Setup the Gemini Observation DB to use our PostgreSQL database settings
db_config.using_apache = True
db_config.use_utc = fits_storage_config.use_as_archive
db_config.storage_root = fits_storage_config.storage_root
db_config.z_staging_area = fits_storage_config.z_staging_area
db_config.database_url = fits_storage_config.fits_database
db_config.postgres_database_pool_size = fits_storage_config.fits_database_pool_size
db_config.postgres_database_max_overflow = fits_storage_config.fits_database_max_overflow
