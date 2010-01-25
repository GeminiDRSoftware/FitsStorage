# These are config parameters that are imported into the FitsStorage namespace
# We put them in a separate file to ease install issues


# Configure the path to the storage root here 
storage_root = '/net/wikiwiki/dataflow'

# Configure the path the data postgres database here
fits_dbname = 'fitsdata'
fits_database = 'postgres:///'+fits_dbname

# Configure the site and other misc stuff here
fits_installation = "fits-install"
fits_system_status = "operational"

# Configure the Backup and Logfile Directories here
fits_db_backup_dir = "/data/autoingest"
