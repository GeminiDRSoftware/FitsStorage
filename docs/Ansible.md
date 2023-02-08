## Ansible Installs

The install is managed by running an Ansible play.  This play is wrapped in convenient shell scripts called
`archive_install_internal.sh` and `archive_install_aws.sh` in the `ansible` folder.  The play relies on you 
having a proper secrets setup to handle ssh logins to the remote host.

### Vars

We use various ansible vars to control how the host will be setup.  These variables
determine if a host will run `postfix` and if the `preview` and/or `export` services 
will be enabled.  They indicate if a host is running in Chile or Hawaii or on AWS 
and more.  The default variables can all be found in `ansible/playbooks/roles/archive/defaults/main.yml`.
Each inventory overrides only the variables that it needs.  Note that if you redefine
a boolean variable in a .ini format file *you must use capitalized False and True*.

Here is a list of the current variables that we use:

```
enable_all_ingest: false
enable_all_preview: false
enable_export: false
enable_postfix: false
enable_preview: false
is_archive: false
is_aws: false
is_chile: false
is_hawaii: false
is_onsite: true
is_ops: false
is_s3: false
build_sphinx: false
sphinx_opts: '-Q'
```

### AWS Vars

For the AWS deploys, we need to configure the server to access the Amazon S3 service.  This is sensitive,
so the access tokens are not in the repository.  Instead, you need to create an `aws_vars.yml` file in the
`ansible` folder with the appropriate settings.  The file contents are like the following.  See Paul or 
myself (Oliver) or an existing Amazon server if you need these values.

```
using_s3: True
s3_bucket_name: gemini-archive
s3_backup_bucket_name: gemini-archive-backup
s3_staging_area: /data/s3_staging
aws_access_key: <access_key>
aws_secret_key: <secret_key>
```

### Running the install

To install, simply:

```
cd ansible
bash ./archive_install_internal.sh -i dev
```

The `dev` file is setup to install to `hbffits-lv4.hi.gemini.edu`.  There are other files such 
as `dev-centos7` to deploy to a CentOS 7 host on `hbffits-lv1.hi.gemini.edu`.  There is also 
a `dev-aws` file for deploying to `arcdev.gemini.edu` in conjunction with 
`archive_install_aws.sh`.

Once the install finishes, you should be able to browse the deployed site at:

http://hbffits-lv4.hi.gemini.edu/searchform/

### Idempotence

The ansible install is idempotent.  This means that you can rerun the install against the 
target host multiple times without issue.

### EPEL

The ansible install depends on the target host having the EPEL repos setup.  While I had this
in the ansible play, ITS in Chile prefers to setup the EPEL repo themselves on the VM.  So
this may not be something we want in Ansible, but it is worth noting that without EPEL there
will be missing packages during the install.

### CRON

The cron setup is too custom and dangerous to do via Ansible at this time, so I've taken it
back out.  Additionally, we had problems where it was not running correctly when installed
as root, and not installable as fitsdata.  It is best for now to set this up by hand based 
on an existing server in that role.
