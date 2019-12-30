## Configuration

Configuration of the FITS Storage server is evolving and is currently a multi-tiered approach.
The configuration is driven by the `fits_storage/fits_storage_config.py` file.  Most of the
settings in this file are hardcoded and will be the same in any deployment.  For some, the
configuration relies on a method called `lookup_config`.  With that method, the configuration
uses the multi-tier approach.

### Environment Variables

First, it looks for an environment variable with the passed in name (capitalized).  For instance,
if you are looking for setting `foo` it will check for an environment variabled named `FOO`.  If
the environment variable is found, it uses that value.  This is the primary mechanism used by
the Docker images to tweak the container settings.

### ETC File

Next, it looks for a file named `/etc/fiststorage.conf`.  This will look for a section called
`[FitsStorge]` and read settings from there.  This file is built by ansible in the deploy and is 
basically used so we can put the s3 related access keys outside of the source tree.  This file
looks something like:

```
[FitsStorage]
using_s3: True
s3_bucket_name = some bucket name
s3_backup_bucket_name = some backup bucket name
s3_staging_area = path to s3 staging area on host
aws_access_key = the access key
aws_secret_key = the secret key
```

### Host-specific Settings

If neither of those has the setting we are looking for, it will fall back to a dictionary
of host-specific values defined at the top of the file.  This allows us to do things like
change the displayed title for a specific host.  These settings at the top of the file 
look something like:

```
_host_based_configs = {
    "hbffits-lv4": {
        'USE_AS_ARCHIVE': 'False',
        'EXPORT_DESTINATIONS': '',
        'PUBDB_REMOTE': 'https://localhost/ingest_publications',
        'BLOCKED_URLS': '',
        'FITS_SERVERTITLE': 'TEST On-site FitsServer',
        'FITS_SYSTEM_STATUS': 'development'
    },
    ...
```

### Defaults

Finally, if none of those places have the setting we are looking for, we return the default
that was passed into the call.
