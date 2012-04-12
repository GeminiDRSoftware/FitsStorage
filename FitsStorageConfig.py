# These are config parameters that are imported into the FitsStorage namespace
# We put them in a separate file to ease install issues

# NOTE: all setting may be overwritten if environment variable,
#   FITSSTORAGECONFIG_LOCALMODE, is true

# controls if the system operates in regular FITSSTORE mode or Local Mode, as used by
# the Astrodata Recipe System.
fsc_localmode = False 

# Configure the path to the storage root here 
storage_root = '/data/dataflow'
#storage_root = '/net/wikiwiki/dataflow'
#storage_root = '/net/hahalua/data/export'

target_gb_free = 100
target_max_files = 125000

# This is the path in the storage root where processed calibrations
# uploaded through the http server get stored.
upload_staging_path = "/data/upload_staging"
processed_cals_path = "reduced_cals"

# The DAS calibration reduction path is used to find the last processing
# date for the gmoscal page's autodetect daterange feature
das_calproc_path = '/net/endor/export/home/dataproc/data/gmos/'
#das_calproc_path = '/net/josie/staging/dataproc/gmos'

# Configure the site and other misc stuff here
fits_servername = "hbffits3"
fits_system_status = "development"

email_errors_to = "phirst@gemini.edu"

# Configure the path the data postgres database here
fits_dbname = 'fitsdata'
fits_database = 'postgresql:///'+fits_dbname

# Configure the Backup Directory here
fits_db_backup_dir = "/data/backups"

# Configure the LockFile Directory here
fits_lockfile_dir = "/data/logs"

# Configure the log directory here
fits_log_dir = "/data/logs/"

# Configure the tape scratch directory here
fits_tape_scratchdir = "/data/tapescratch"


# the following implements allows astrodata to set local versions of 
# setting in this module
import os,sys
if "FITSSTORAGECONFIG_LOCALMODE" in os.environ:
    fsc_lm = os.environ["FITSSTORAGECONFIG_LOCALMODE"].strip().lower()
    if fsc_lm == "true":
        from astrodata import fscpatch
        fscpatch.local_fsc_patch(sys.modules[__name__])
