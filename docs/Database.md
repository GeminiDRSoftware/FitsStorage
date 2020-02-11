
# Schema Creation

This is normally done during the initial server setup.  This uses the `create_tables.py`
script from the codebase.  If you need to run it manually, the command is.

```
python fits_storage/scripts/create_tables.py
```

After creating the tables, you will want to grant `fitsdata` and `apache` access to the
tables and sequences.

```
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fitsdata   
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fitsdata   
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO apache   
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO apache   
```

# Backup

Doing a normal backup can use pg_dump and the binary format.  This is suitable for
restoring back into a Postgres database of the same version level.

```
/usr/bin/pg_dump --format=c --file=fitsdata.DATE.pg_dump_c fitsdata
```

# Restore

For the raw backups, the postgres restore command is just:

```
/usr/bin/pg_restore --dbname=fitsdata --format=c /data/backups/fitsdata.DATE.pg_dump_c
```

# Migration

For migrating data to another instance that already has the tables, you can
use plain format and dump data only

```
/usr/bin/pg_dump --data-only --format=p fitsdata | gzip -7 > fitsdata.DATE.pg_dump_p.gz
```

On the target system, you can then clear out the tables and restore with

```
zcat fitsdata.DATE.pg_dump_p.gz | /usr/bin/psql -d fitsdata -f -
```

this will likely generate some permissions errors which can be ignored.

# Archive to On-Site Migration

For an ansible play to do these steps, look at `rollcall.yml`.

Previews and DiskFiles will all be .bz2 files, which is not true of the onsite servers.
First, we can fix the previews:

```
update preview set filename=REGEXP_REPLACE(filename, '(.*).bz2_preview.jpg', '\1_preview.jpg') where filename like '%bz2_preview.jpg';
```

Next, we can fix the diskfiles.  To start, we set the file md5 and size to match the data 
md5 and size, since for raw .fits files that will be the case.

```
update DiskFile set file_md5=data_md5 where present=TRUE;
```

Then we also fix the filenames as we did for Previews

```
update DiskFile set filename=REGEXP_REPLACE(filename, '(.*).bz2', '\1') where filename like '%bz2';
```

We can also delist older files 

```
UPDATE diskfile SET present=FALSE WHERE filename like 'N2013%'
```

As well as whichever site is not the one we are setting up

```
UPDATE diskfile SET present=FALSE WHERE filename like '[S OR N]%'
```
