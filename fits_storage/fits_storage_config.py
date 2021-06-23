"""
These are config parameters that are imported into the FitsStorage namespace
We put them in a separate file to ease install issues
"""

import os
import socket
import configparser

""" Configuration defaults based on the hostname """


def get_hostname():
    hostname = socket.gethostname()
    if hostname is not None and '.' in hostname:
        hostname = hostname[:hostname.find('.')]
    return hostname


def get_fits_db_backup_dir():
    hostname = get_hostname()
    if hostname is None or hostname == 'arcdev' or hostname == 'archive':
        return '/backup'
    return "/sci/dataflow/FitsStorage_Backups/%s" % hostname
    

_config = None


def lookup_config(name, default_value):
    """ Lookup a config value with the given name and a default.

    Config values are returned via a 3-tiered lookup.  First, if the
    name is found in the environment, that is used to get the value.
    If not, it next looks at the _host_based_configs for this host
    to see if there is a host-specific value for that same name.
    Finally, it returns the default.
    """
    # TODO singleton wrap this - cheap anyway and will just be used at startup
    # try to load /etc/fitsstorage.conf
    # but be resilient if it is not found
    global _config

    if _config is None:
        config_path = os.getenv('FITSSTORAGE_CONFIG_FILE', None)
        if config_path is None:
            hostname = socket.gethostname()
            if hostname is not None and '.' in hostname:
                hostname = hostname[:hostname.find('.')]
            if hostname is not None:
                if hostname.startswith('cpo') or hostname.startswith('mko') or hostname.startswith('hbf') or \
                        hostname == 'arcdev' or hostname == 'archive':
                    config_path = '/etc/fitsstorage.conf'
                else:
                    config_path = '~/fitsstorage.conf'

        _config = configparser.ConfigParser()
        if os.path.exists(config_path):
            _config.read(config_path)
    env_value = os.getenv(name, None)
    if env_value is not None:
        # we found it via the environment, this takes precedence
        return env_value
    if 'FitsStorage' in _config:
        if name.lower() in _config['FitsStorage']:
            return _config['FitsStorage'][name.lower()]

    return default_value


def lookup_config_bool(name, default_value):
    retval_str = lookup_config(name, None)
    if retval_str is None:
        return default_value
    return retval_str.lower() in ['true', 't', '1', 'y', 'yes']


# Is this an archive server
use_as_archive = lookup_config_bool('USE_AS_ARCHIVE', False)

# AWS S3 info
using_s3 = lookup_config_bool('USING_S3', False)
s3_bucket_name = lookup_config('S3_BUCKET_NAME', '')
s3_backup_bucket_name = lookup_config('S3_BACKUP_BUCKET_NAME', '')
s3_staging_area = lookup_config('S3_STAGING_AREA', '')
aws_access_key = lookup_config('AWS_ACCESS_KEY', '')
aws_secret_key = lookup_config('AWS_SECRET_KEY', '')

# Staging area for uncompressed cache of compressed file being processed
z_staging_area = lookup_config('Z_STAGING_AREA', '/data/z_staging')

# Configure the path to the storage root here 
storage_root = lookup_config('STORAGE_ROOT', '/sci/dataflow')
dhs_perm = lookup_config('DHS_PERM', '/sci/dhs')
min_dhs_age_seconds = 15
max_dhs_validation_failures = 4

if(using_s3):
    storage_root = s3_staging_area

# Defer ingestion of files modified within the last defer_seconds seconds
# for at least a further defer_seconds seconds
# Set to zero to disable
defer_seconds = 4


# Target free space and number of files on storage_root for delete script
target_gb_free = 250
target_max_files = 8000000
delete_min_days_age = 365

# This is the path in the storage root where processed calibrations
# uploaded through the http server get stored.
upload_staging_path = lookup_config('UPLOAD_STAGING_PATH', '/data/upload_staging')

# This is the cookie value needed to allow uploading files.
# Leave it empty to disable upload authentication
# The cookie name is 'gemini_fits_upload_auth'
upload_auth_cookie = lookup_config('UPLOAD_AUTH_COOKIE', 'f3c6986fddfe42a8ce117203924c6983')

# This is the cookie supplied to servers we are exporting to.
export_upload_auth_cookie = lookup_config('EXPORT_UPLOAD_AUTH_COOKIE', 'f3c6986fddfe42a8ce117203924c6983')


# This is the magic cookie value needed to allow downloading any files
# without any other form of authentication
# The cookie name is 'gemini_fits_authorization'
# Leave it as None to disable this feature
magic_download_cookie = 'good_to_go'

# This is the magic cookie value needed to allow API access without
# any other authentication.
# Leave it as None to disable this feature
magic_api_cookie = 'f0a49ab56f80da436b59e1d8f20067f4'

# API backend stuff
provides_api = True
api_backend_location = lookup_config('API_BACKEND_LOCATION', 'localhost:8000')

# This is the list of downstream servers we export files we ingest to
export_destinations_str = lookup_config('EXPORT_DESTINATIONS', None)
if export_destinations_str is not None and export_destinations_str.strip() != "":
    export_destinations = export_destinations_str.split(',')
else:
    export_destinations = []

# Do we want to bzip2 files we export on the fly?
export_bzip = True

# This is the subdirectory in dataroot where processed_cals live
# Ops hosts override this to reduced_cals
processed_cals_path = lookup_config('PROCESSED_CALS_PATH', "reduced_cals_dev")
default_upload_path = lookup_config('DEFAULT_UPLOAD_PATH', '')

# This is the subdirectory in dataroot where preview files live
using_previews = lookup_config_bool('USING_PREVIEWS', False)
preview_path = "previews"

# The DAS calibration reduction path is used to find the last processing
# date for the gmoscal page's autodetect daterange feature
das_calproc_path = lookup_config('DAS_CALPROC_PATH', '/sci/dasgmos')

# Configure the site and other misc stuff here
# Especially for archive systems, make the servername a fully qualified domain name.
fits_servertitle = lookup_config('FITS_SERVERTITLE', "CPO Fits Server")
fits_servername = lookup_config('FITS_SERVERNAME', socket.gethostname())  # "cpofits-lv2"
fits_system_status = lookup_config('FITS_SYSTEM_STATUS', "production")

# Limit on number of results in open searches
fits_open_result_limit = 500
fits_closed_result_limit = 10000

smtp_server = "localhost"
email_errors_to = "ooberdorf@gemini.edu"

# Configure the path the data postgres database here
fits_dbname = lookup_config('FITS_DB_NAME', 'fitsdata')
fits_dbserver = lookup_config('FITS_DB_SERVER', '')
fits_database = 'postgresql://%s/%s' % (fits_dbserver, fits_dbname)
fits_database_pool_size = 30
fits_database_max_overflow = 10

# Configure the auxillary data directory here
fits_aux_datadir = lookup_config('FITS_AUX_DATADIR', "/opt/FitsStorage/data")

# Configure the template directory here
template_root = fits_aux_datadir + "/templates"

# Configure the Backup Directory here
fits_db_backup_dir = lookup_config('FITS_DB_BACKUP_DIR', get_fits_db_backup_dir())

# Configure the LockFile Directory here
fits_lockfile_dir = lookup_config('FITS_LOCKFILE_DIR', "/data/logs")

# Configure the log directory here
fits_log_dir = lookup_config('FITS_LOG_DIR',"/data/logs/")

# Indicate if we are running in a docker context (to alter logging behavior)
is_docker_str = lookup_config('IS_DOCKER', "False")
if is_docker_str is not None and is_docker_str == "True":
    is_docker = True
else:
    is_docker = False

# Configure the tape scratch directory here
fits_tape_scratchdir = lookup_config('TAPESCRATCH', "/data/tapescratch")

# Configure install specifics such as database backend tweaks, apache presence, etc
# fsc_localmode is depreciated
using_apache = True
using_sqlite = False

# Publication database connection info
pubdb_host = 'hbfmysql1.hi.gemini.edu'
pubdb_username = 'fitsdata'
pubdb_dbname = 'apps-publications'
pubdb_remote = lookup_config('PUBDB_REMOTE', 'https://localhost/ingest_publications')

# By default, all URLs on the server are active. List in blocked_urls any that you want to disable
blocked_urls_str = lookup_config('BLOCKED_URLS', 'debug,summary,diskfiles,ssummary,lsummary,standardobs,calibrations,xmlfilelist,fileontape,calmgr,upload_processed_cal,fitsverify,mdreport,fullheader,file,programsobserved,gmoscal,qareport,qametrics,qaforgui,stats,tape,tapewrite,tapefile,taperead,xmltape,notification,curation,observing_statistics,authentication')
if blocked_urls_str is not None:
    blocked_urls = blocked_urls_str.split(',')
else:
    blocked_urls = []

validation_def_path = lookup_config('VALIDATION_DEF_PATH', '/opt/FitsStorage/docs/dataDefinition')

logreports_use_materialized_view = lookup_config_bool('LOGREPORTS_USE_MATERIALIZED_VIEW', 'True')

orcid_client_id = lookup_config('ORCID_CLIENT_ID', "APP-DBYNSNN5SCGF2G75")
orcid_client_secret = lookup_config('ORCID_CLIENT_SECRET', "66f21bdb-90f6-4219-bd81-f6b6af3bf34a")
orcid_server = lookup_config('ORCID_SERVER', 'sandbox.orcid.org')
orcid_enabled = lookup_config_bool('ORCID_ENABLED', True)
orcid_redirect_url = lookup_config('ORCID_REDIRECT_URL', 'http://%s/orcid' % fits_servername)
