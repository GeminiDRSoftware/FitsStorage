# Archive Promotion Checklist

This is just a checklist for me to work through as we upgrade the `arcdev` host to be the new
`archive` host.

## Bring Down Export On CPO/MKO

I don't think there are 2 queues, but be safe:

```
systemctl stop fits-service_export_queue1
systemctl stop fits-service_export_queue2
```

## Bring Down Archive Services

```
systemctl stop fits-service_preview_queue1
systemctl stop fits-service_ingest_queue1
systemctl stop fits-service_ingest_queue2
systemctl stop fits-cal_cache_queue1
systemctl stop fits-cal_cache_queue2
systemctl stop fits-httpd
systemctl stop fits-api-backend.service
ps -Aef | grep fits
```

## Bring Down ArcDev Services

```
systemctl stop fits-service_preview_queue1
systemctl stop fits-service_ingest_queue1
systemctl stop fits-service_ingest_queue2
systemctl stop fits-cal_cache_queue1
systemctl stop fits-cal_cache_queue2
systemctl stop fits-httpd
systemctl stop fits-api-backend.service
ps -Aef | grep fits
```

## Copy Out Additional Tables

```
/usr/bin/pg_dump --data-only --format=p -t archiveuser -t archiveuser_id_seq -t userprogram -t userprogram_id_seq -t glacier -t glacier_id_seq  -t downloadlog -t downloadlog_id_seq -t filedownloadlog -t filedownloadlog_id_seq -t fileuploadlog -t fileuploadlog_id_seq -t querylog -t querylog_id_seq -t usagelog -t usagelog_id_seq fitsdata | gzip -2 > metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz
```

## SCP To ArcDev Host

```
scp metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz username@arcdev.gemini.edu:
```

## Import Data Into ArcDev DB

```
sudo -u fitsdata psql fitsdata
```

```
truncate table filedownloadlog
truncate table fileuploadlog
truncate table querylog cascade
truncate table usagelog cascade
truncate table downloadlog
truncate table archiveuser
truncate table userprogram
truncate table glacier
```

```
zcat metricsandlogs-arc-YYYYMMDD.pg_dump_p.gz | /usr/bin/psql -d fitsdata -f -
```

## Edit /etc/fitsstore.conf

May not be required with an ansible deploy

Set Name to something nice
Set mode to production

## Redeploy FitsStorage

This can be rsync over ssh, or just ansible it.  Here it is targetting the host as
`arcdev`, but we could update to `archive` if we want to repoint it ahead of time.

```
pip3 install ansible
git checkout 2020-1
bash ./archive_install_aws.sh -i dev-aws
```

## Validate Services Running

```
ps -Aef | grep fits
```

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

## Repoint IP

This lets us hit the site with the `archive` name, and is required before
Let's Encrypt will work.

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

## Check HTTPS Server Access

https://archive.gemini.edu

## Start Export On CPO/MKO

I don't think there are 2 queues, but be safe:

```
systemctl start fits-service_export_queue1
systemctl start fits-service_export_queue2
```

## Start Services on Archive

```
systemctl start fits-service_preview_queue1
systemctl start fits-service_ingest_queue1
systemctl start fits-service_ingest_queue2
systemctl start fits-cal_cache_queue1
systemctl start fits-cal_cache_queue2
systemctl start fits-httpd
systemctl start fits-api-backend.service
ps -Aef | grep fits
```