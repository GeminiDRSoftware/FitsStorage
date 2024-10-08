# Backup and Restore

These notes cover backing up and restoring the archive/fits store.

## Backup

`/usr/bin/pg_dump --format=c --file=fitsdata.DATE.pg_dump_c fitsdata`

## Restore

`/usr/bin/pg_restore --dbname=fitsdata --format=c /data/backups/fitsdata.DATE.pg_dump_c`

## Format Notes

Using`--format=c` is best, but won't work across postgres versions. `format=p` (plain)
outputs a plain text SQL script which should work accross versions.
But these can be very large and you probably want to compress it, so:

`/usr/bin/pg_dump --format=p fitsdata | gzip -7 > fitsdata.DATE.pg_dump_p.gz`

or data only

`/usr/bin/pg_dump --data-only --format=p fitsdata | gzip -7 > fitsdata.DATE.pg_dump_p.gz`

restore this with:
`gunzip -c fitsdata.DATE.pg_dump_p.gz | /usr/bin/psql -d fitsdata -f -`

this will likely generate some permissions errors which can be ignored.
