# Archive Promotion Checklist

This is just a checklist for me to work through as we upgrade the `arcdev` host to be the new
`archive` host.

## Bring Down Archive Services

```
systemctl stop fits-service_preview_queue1
systemctl stop fits-service_ingest_queue1
systemctl stop fits-service_ingest_queue2
systemctl stop fits-httpd
systemctl stop fits-api-backend.service
ps -Aef | grep fits
```

## Bring Down ArcDev Services

```
systemctl stop fits-service_preview_queue1
systemctl stop fits-service_ingest_queue1
systemctl stop fits-service_ingest_queue2
systemctl stop fits-httpd
systemctl stop fits-api-backend.service
ps -Aef | grep fits
```

## Copy Out Additional Tables

```
/usr/bin/pg_dump --data-only --format=p -t archiveuser -t archiveuser_id_seq -t userprogram -t userprogram_id_seq -t glacier -t glacier_id_seq  -t downloadlog -t downloadlog_id_seq -t filedownloadlog -t filedownloadlog_id_seq -t fileuploadlog -t fileuploadlog_id_seq -t qareport -t qareport_id_seq -t qametriciq -t qametriciq_id_seq -t qametriczp -t qametriczp_id_seq -t qametricsb -t qametricsb_id_seq -t qametricpe -t qametricpe_id_seq -t usagelog -t usagelog_id_seq fitsdata | gzip -7 > metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz
```

or use format=c for compatible editions of postgres

## SCP To ArcDev Host

```
scp metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz username@arcdev.gemini.edu:
```

## Import Data Into ArcDev DB

### Truncate the destinations

```
sudo -u fitsdata psql fitsdata
```

```
truncate table qametricpe
truncate table qametricsb
truncate table qametriczp
truncate table qametriciq
truncate table qareport cascade
truncate table usagelog cascade
truncate table downloadlog
truncate table archiveuser
truncate table userprogram
truncate table glacier
```

### Drop indices for performance

```
drop index ix_glacier_filename;
drop index ix_glacier_md5;

drop index ix_usagelog_ip_address;
drop index ix_usagelog_status;
drop index ix_usagelog_this;
drop index ix_usagelog_utdatetime;

drop index ix_querylog_summarytype;

drop index ix_filedownloadlog_diskfile_filename;

drop index ix_fileuploadlog_filename;
```

### Do the restore

```
zcat metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz | /usr/bin/psql -d fitsdata -f -
```

or

```
pg_restore --dbname=fitsdata --format=c --jobs=8 metricsandlogs-arc-YYYYMMDD.pg_dump_c
```

### Rebuild the indices

```
create index ix_glacier_filename on glacier(filename);
create index ix_glacier_md5 on glacier(md5);

create index ix_usagelog_ip_address on usagelog(ip_address);
create index ix_usagelog_status on usagelog(status);
create index ix_usagelog_this on usagelog(this);
create index ix_usagelog_utdatetime on usagelog(utdatetime);

create index ix_querylog_summarytype on querylog(summarytype);

create index ix_filedownloadlog_diskfile_filename on filedownloadlog(diskfile_filename);

create index ix_fileuploadlog_filename on fileuploadlog(filename);
```

## Edit /etc/fitsstore.conf

May not be required with an ansible deploy

Set Name to something nice
Set mode to production

## Redeploy FitsStorage

rsync over ssh, or just ansible it

## Validate Services Running

ps -Aef | grep fits

## Run Ingest On New Data

```
sudo -u fitsdata env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --file-re [NS]202003
```

## Check Timeouts

Should be handled by ansible

/opt/modwsgi-default/httpd.conf
In the WSGIDaemon sections (2 of them) only
Increase socket-timeout and request-timeout from 60 to 3600

## Check/Enable CRON Jobs

## Redo Let's Encrypt Cert

Set hostname to archive and start/restart webserver

Then:

```
sudo yum install -y epel-release
sudo yum install -y certbot python3-certbot-apache mod_ssl
sudo certbot certonly --webroot -w /opt/modwsgi-default/htdocs/ -d archive.gemini.edu
```

Check/fix/add cron job

Should have been automatic, see: https://techmonger.github.io/49/certbot-auto-renew/
