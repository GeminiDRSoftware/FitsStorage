"""
These are config parameters that are imported into the FitsStorage namespace
We put them in a separate file to ease install issues
"""

import os, sys

# NOTE: all setting may be overwritten if environment variable,
#   FITSSTORAGECONFIG_LOCALMODE, is true

# Is this an archive server
#use_as_archive = True
use_as_archive = False

# AWS S3 info
using_s3 = False
s3_bucket_name = 'gemini-archive'
s3_staging_area = '/data/FitsStorage/s3_staging'
aws_access_key = 'AKIAJ554XPYMJZBPGQAA'
aws_secret_key = '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'

# Staging area for uncompressed cache of compressed file being processed
z_staging_area = '/data/FitsStorage/z_staging'

# Configure the path to the storage root here 
storage_root = '/data/FitsStorage/dataflow'
#storage_root = '/net/wikiwiki/dataflow'
#storage_root = '/net/hahalua/data/export'

if(using_s3):
    storage_root = s3_staging_area

# Defer ingestion of files modified within the last defer_seconds seconds
# for at least a further defer_seconds seconds
# Set to zero to disable
defer_seconds = 30


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

# This is the magic cookie value needed to allow downloading any files
# without any other form of authentication
# The cookie name is 'gemini_fits_authorization'
# Leave it as None to disable this feature
magic_download_cookie = None

# This is the list of downstream servers we export files we ingest to
export_destinations = []
#export_destinations = ['hbffits2']

# Do we want to bzip2 files we export on the fly?
export_bzip = True

# This is the subdirectory in dataroot where processed_cals live
processed_cals_path = "reduced_cals"

# This is the subdirectory in dataroot where preview files live
using_previews = True
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

# Configure the Backup Directory here
fits_db_backup_dir = "/data/FitsStorage/backups"

# Configure the LockFile Directory here
fits_lockfile_dir = "/data/FitsStorage/logs"

# Configure the log directory here
fits_log_dir = "/data/FitsStorage/logs/"

# Configure the tape scratch directory here
fits_tape_scratchdir = "/data/tapescratch"

# Configure install specifics such as database backend tweaks, apache presence, etc
# fsc_localmode is depreciated
using_apache = True
using_sqlite = False


# This is used to reference program keys with the odb
odbkeypass = "dontputtheactualkeyinthesvn"

# By default, all URLs on the server are active. List in blocked_urls any that you want to disable
blocked_urls = []
#blocked_urls=['debug', 'summary', 'diskfiles', 'ssummary', 'lsummary', 'standardobs', 'calibrations', 'xmlfilelist', 'fileontape', 'calmgr', 'upload_processed_cal', 'fitsverify', 'mdreport', 'fullheader', 'file', 'programsobserved', 'gmoscal', 'qareport', 'qametrics', 'qaforgui', 'stats', 'tape', 'tapewrite', 'tapefile', 'taperead', 'xmltape', 'notification', 'curation', 'observing_statistics', 'authentication']

# the following implements allows astrodata to set local versions of 
# setting in this module
if "FITSSTORAGECONFIG_LOCALMODE" in os.environ:
    fsc_lm = os.environ["FITSSTORAGECONFIG_LOCALMODE"].strip().lower()
    if fsc_lm == "true":
        from astrodata import fscpatch
        fscpatch.local_fsc_patch(sys.modules[__name__])

validation_def_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docs/dataDefinition')
