"""
These are config parameters that are imported into the FitsStorage namespace
We put them in a separate file to ease install issues
"""

import os, sys

# Is this an archive server
use_as_archive = True
#use_as_archive = False

# AWS S3 info
using_s3 = False
s3_bucket_name = ''
s3_backup_bucket_name = ''
s3_staging_area = '/data/s3_staging'
aws_access_key = ''
aws_secret_key = ''

# Staging area for uncompressed cache of compressed file being processed
z_staging_area = '/data/z_staging'

# Configure the path to the storage root here 
#storage_root = '/net/wikiwiki/dataflow'
storage_root = '/data/gemini_data'

if(using_s3):
    storage_root = s3_staging_area

# Defer ingestion of files modified within the last defer_seconds seconds
# for at least a further defer_seconds seconds
# Set to zero to disable
defer_seconds = 0


# Target free space and number of files on storage_root for delete script
target_gb_free = 100
target_max_files = 125000

# This is the path in the storage root where processed calibrations
# uploaded through the http server get stored.
upload_staging_path = "/data/upload_staging"

# This is the cookie value needed to allow uploading files.
# Leave it empty to disable upload authentication
# The cookie name is 'gemini_fits_upload_auth'
upload_auth_cookie = None

# This is the cookie supplied to servers we are exporting to.
export_upload_auth_cookie = None


# This is the magic cookie value needed to allow downloading any files
# without any other form of authentication
# The cookie name is 'gemini_fits_authorization'
# Leave it as None to disable this feature
magic_download_cookie = None

# This is the magic cookie value needed to allow API access without
# any other authentication.
# Leave it as None to disable this feature
magic_api_cookie = None

# API backend stuff
provides_api = True
api_backend_location = 'localhost:8000'

# This is the list of downstream servers we export files we ingest to
export_destinations = []
#export_destinations = ['hbffits2']

# Do we want to bzip2 files we export on the fly?
export_bzip = True

# This is the subdirectory in dataroot where processed_cals live
processed_cals_path = "reduced_cals"

# This is the subdirectory in dataroot where preview files live
using_previews = False
preview_path = "previews"

# The DAS calibration reduction path is used to find the last processing
# date for the gmoscal page's autodetect daterange feature
das_calproc_path = '/net/endor/export/home/dataproc/data/gmos/'
#das_calproc_path = '/net/josie/staging/dataproc/gmos'

# Configure the site and other misc stuff here
# Especially for archive systems, make the servername a fully qualified domain name.
fits_servername = "hahalua"
fits_system_status = "development"

# Limit on number of results in open searches
fits_open_result_limit = 500
fits_closed_result_limit = 10000

smtp_server = "smtp.gemini.edu"
email_errors_to = "phirst@gemini.edu"

# Configure the path the data postgres database here
fits_dbname = 'fitsdata'
fits_database = 'postgresql:///'+fits_dbname
#fits_database = 'sqlite:////home/fitsdata/sqlite-database'
#To reference database on another machine: 
#fits_database = 'postgresql://hbffitstape1/'+fits_dbname

# Configure the auxillary data directory here
fits_aux_datadir = "/opt/FitsStorage/data"

# Configure the template directory here
template_root = fits_aux_datadir + "/templates"

# Configure the Backup Directory here
fits_db_backup_dir = "/data/backups"

# Configure the LockFile Directory here
fits_lockfile_dir = "/data/logs"

# Configure the log directory here
fits_log_dir = "/data/logs/"

# Configure the tape scratch directory here
fits_tape_scratchdir = "/data/tapescratch"

# Configure install specifics such as database backend tweaks, apache presence, etc
# fsc_localmode is depreciated
using_apache = True
using_sqlite = False

# Publication database connection info
pubdb_host = 'hbfmysql1.hi.gemini.edu'
pubdb_username = 'fitsdata'
pubdb_password = 'jpPyKE56H4ctVKVL'
pubdb_dbname = 'apps-publications'

# This is used to reference program keys with the odb
odbkeypass = "dontputtheactualkeyinthesvn"

# By default, all URLs on the server are active. List in blocked_urls any that you want to disable
blocked_urls = []
#blocked_urls=['debug', 'summary', 'diskfiles', 'ssummary', 'lsummary', 'standardobs', 'calibrations', 'xmlfilelist', 'fileontape', 'calmgr', 'upload_processed_cal', 'fitsverify', 'mdreport', 'fullheader', 'file', 'programsobserved', 'gmoscal', 'qareport', 'qametrics', 'qaforgui', 'stats', 'tape', 'tapewrite', 'tapefile', 'taperead', 'xmltape', 'notification', 'curation', 'observing_statistics', 'authentication']

validation_def_path = '/opt/FitsStorage/docs/dataDefinition'
