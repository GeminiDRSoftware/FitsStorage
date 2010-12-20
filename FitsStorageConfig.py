# These are config parameters that are imported into the FitsStorage namespace
# We put them in a separate file to ease install issues


# Configure the path to the storage root here 
storage_root = '/net/hahalua/data/export'
#storage_root = '/net/wikiwiki/dataflow'
#storage_root = "/data/dataflow"

# This is the path in the storage root where processed calibrations
# uploaded through the http server get stored.
upload_staging_path = "/data/upload_staging"
processed_cals_path = "reduced_cals"

# The DAS calibration reduction path is used to find the last processing
# date for the gmoscal page's autodetect daterange feature
das_calproc_path = '/net/endor/export/home/dataproc/data/gmos/'
#das_calproc_path = '/net/josie/staging/dataproc/gmos'

# Configure the site and other misc stuff here
fits_servername = "hbffits1"
fits_system_status = "development"

# Configure the path the data postgres database here
fits_dbname = 'fitsdata'
fits_database = 'postgresql:///'+fits_dbname

# Configure the Backup Directory here
fits_db_backup_dir = "/data/backups"

# Configure the LockFile Directory here
fits_lockfile_dir = "/data/logs"

# Configure the log directory here
fits_log_dir = "/data/logs/"

# Configure the tape device here
fits_tape_device = "/dev/nst0"

# Configure the tape scratch directory here
fits_tape_scratchdir = "/data/tapescratch"

