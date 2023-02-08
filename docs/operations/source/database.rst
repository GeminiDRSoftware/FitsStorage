Database integrity 
==================

If you're seeing problems that seem related to one or a few files only, it's possible that somehow the entries for those files
in the database have got messed up somehow. In order to fix this, you'll have to manipulate the database directly by issuing
SQL commands from the psql interface while logged in as fitsdata. You should be an expert on the database layout and how
files are ingested if you're going to do this.

Given that the database can only get messed up like this by a software bug, and we fix the bugs when we find them, it's difficult
to predict here what might have happened. One example that we have seen in the past is where somehow two ingest threads attempt to
ingest the same file simultaneously. The Queue system should not allow this to ahppen, but if it does, then you will get two
entries in the diskfile table that both refer to the same file_id, yet both have canonical=True and present=True, which should
never occur. Attempts to download this file would fail with an exception raised by a call to query.one() where only one result was
expected. The solution in this case is to simply update one of the datafile rows to set the present and canonical columns to False.

In general, the database schema prevents a lot of issues like this from ever happening by enforcing constraints in the database
itself - so you'll get an exception when the buggy code tries to do something it shouldn't. There are some cases where this is not
the case though. However, there is a web interface that will do some basic database curation checks. You'll need to be logged in with
staff access to get access to it, and it can be found at the /curation URL.

Database Backups
++++++++++++++++

There is a cron job that runs pg_dump on the database daily, to a directory specified in the fits_storage_config.py file.
If the database gets really corrupted somehow, then you may have to restore it from the previous good backup. The backup script
includes a datestamp in the names of the dumped files so you can easily tell when a backup is from. In addition, by default it deletes old
backup dumps according to an algorithm that preserves daily dumps for the last 10 days, weekly dumps for a few months, and monthly dumps further
back than that. This prevents the dumps directory from getting excessive in size too quickly.

Manual backup / restore operations
----------------------------------

Backup manually with:
    /usr/bin/pg_dump --format=c --file=fitsdata.DATE.pg_dump_c fitsdata


Restore manually with:
    /usr/bin/pg_restore --dbname=fitsdata --format=c /data/backups/fitsdata.DATE.pg_dump_c

Note that restore will be faster if you use the --jobs argument appropriately for the machine you're on. Also you can restore a 
subset of the tables using the --table argument. See the man page for pg_restore for details.


Note: --format=c is best, but won't work across postgres versions. format=p (plain) outputs a plain text SQL script which should 
work accross versions.  But these can be very large and you probably want to compress it, so something like:

    /usr/bin/pg_dump --format=p fitsdata | gzip -7 > fitsdata.DATE.pg_dump_p.gz

This restores with:
    gzcat fitsdata.DATE.pg_dump_p.gz | /usr/bin/psql -d fitsdata -f -

this will likely generate some permissions errors which can be ignored.

