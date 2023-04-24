Don't blow away all the tables, keep:
archiveuser, userprogram, downloadlog, filedownloadlog, fileuploadlog querylog usagelog.
It's better to keep these rather than restore them from backup so that the sequeneces are up to date.
If restore from backup, need to advance the id sequences up to max(id) for each table.

Do a pg_dump after you add_to_ingestqueue for easier restart if necessary.

Deleting previews from s3 takes several hours.
Using 200 2-cpu machines with 3 threads on each worked well. Took about 2 days including doing previews.

Rebuilding the calcache table takes about 4 days on a 4 core machine. Consider doing a database dump and getting a 
16+ core c3 instance to blast through it.
