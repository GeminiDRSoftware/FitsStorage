# These are config parameters that are imported into the FitsStorage namespace
# We put them in a separate file to ease install issues


# Configure the path to the storage root here 
storage_root = '/net/wikiwiki/dataflow'

# Configure the site and other misc stuff here
fits_servername = "fits"
fits_system_status = "operational"

# Configure the path the data postgres database here
fits_dbname = 'fitsdata'
fits_database = 'postgres:///'+fits_dbname

# Configure the Backup Directory here
fits_db_backup_dir = "/data/autoingest"

# Configure the LockFile Directory here
fits_lockfile_dir = "/data/autoingest"

# Configure the log directory here
fits_log_dir = "/data/autoingest/"

# Configure the tape device here
fits_tape_device = "/dev/nst0"

# Configure the tape scratch directory here
fits_tape_scratchdir = "/data/tapescratch"

