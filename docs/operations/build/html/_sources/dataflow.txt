Dataflow through the system
===========================

This section explains how files get moved through the dataflow system, from the DHS filesystem through to the archive,
and how they may get manipulated along the way. In summary:

* On the Summit Fits Server:

  - Files Appear on DHS filesystem
  - copy_from_dhs.py copies them to Dataflow filesystem and adds them to ingest queue
  - service_ingestqueue.py ingests them into the database and adds them to the export queue to go to the archive
  - service_exportqueue.py starts transferring them to the archive

* On the archive

  - apache writes the file to the upload_staging directory
  - ingest_uploaded_file copies the file to the S3 data store and adds it to the ingest queue
  - service_ingestqueue ingests it into the database and adds it to the preview queue and the calcache queue
  - service_previewqueue generates a preview jpg for it
  - service_calcachequeue computes calibration associations for it and stores them in the calibcation association cache



Files originate on the DHS filesystem
+++++++++++++++++++++++++++++++++++++

The DHS or GIAPI is responsible for creating the raw data file on the DHS perm filesystem. This filesystem
is normally automounted on the summit fits servers at /sci/dhs, but it will be mounted in a different place 
on the operations and DHS machines. If the files are not even showing up there, then this document won't help
you and and you need to talk to the software group.

They are picked up by the copy_from_dhs.py task
+++++++++++++++++++++++++++++++++++++++++++++++

When a file appears on the DHS disk, a process running on the Summit Fits Server notices that (by constantly polling
the DHS disk for expected filenames). This process is called ''copy_from_dhs.py'' and is started by systemd, with the
service name fits-copy_from_dhs. It should get automatically restarted (by systemd) if it dies (so long as it doesn't 
just repeatedly die again).

You can check if that task is running by asking systemd::

  [phirst@mkofits-lv1 ~]$ sudo systemctl status fits-copy_from_dhs
  fits-copy_from_dhs.service - Fits Copy From DHS
     Loaded: loaded (/etc/systemd/system/fits-copy_from_dhs.service; enabled)
     Active: active (running) since Fri 2015-12-04 11:13:56 HST; 3 months 13 days ago
   Main PID: 28851 (python)
     CGroup: /system.slice/fits-copy_from_dhs.service
             └─28851 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/copy...


You can also just look at the process list directly, for example::

  [phirst@mkofits-lv1 ~]$ ps aux | fgrep python |fgrep copy_from_dhs
  fitsdata 28851  0.1  0.5 338484 21792 ?        Ss    2015 297:56 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/copy_from_dhs.py --demon --lockfile --debug

And you can see what the process it doing from the log file::

  [phirst@mkofits-lv1 ~]$ tail /data/logs/copy_from_dhs.py.log
  2016-03-18 14:25:12,261 28851:copy_from_dhs:129 DEBUG: Ignoring directory: .snapshot
  2016-03-18 14:25:12,261 28851:copy_from_dhs:136 DEBUG: Ignoring tmp file: .vtoc_internal
  2016-03-18 14:25:12,262 28851:copy_from_dhs:136 DEBUG: Ignoring tmp file: .bplusvtoc_internal
  2016-03-18 14:25:12,262 28851:copy_from_dhs:145 DEBUG: Pass complete, sleeping
  2016-03-18 14:25:17,267 28851:copy_from_dhs:147 DEBUG: Re-scanning
  2016-03-18 14:25:17,272 28851:copy_from_dhs:126 DEBUG: 3 new files to check
  2016-03-18 14:25:17,272 28851:copy_from_dhs:129 DEBUG: Ignoring directory: .snapshot
  2016-03-18 14:25:17,273 28851:copy_from_dhs:136 DEBUG: Ignoring tmp file: .vtoc_internal
  2016-03-18 14:25:17,273 28851:copy_from_dhs:136 DEBUG: Ignoring tmp file: .bplusvtoc_internal
  2016-03-18 14:25:17,273 28851:copy_from_dhs:145 DEBUG: Pass complete, sleeping


When the copy_from_dhs.py task detects a new file on the DHS filesystem, it copies it to the summit dataflow firesystem
and adds it to the ingest queue to be ingested into the database. You can see this happening in the logfile::

  2016-03-18 14:32:00,771 28851:copy_from_dhs:41 INFO: Copying N20160319S0018.fits to /sci/dataflow
  2016-03-18 14:32:00,834 28851:copy_from_dhs:50 INFO: Adding N20160319S0018.fits to IngestQueue

copy_from_dhs.py will attempt to copy any files that are on the DHS filesystem that are not already on the dataflow filesystem, 
so even if it's been down for a while, when it restarts it should catch up just fine. 

The end-point of a file for copy_from_dhs.py is that it has been copied to the dataflow filesystem and placed in the ingest queue.

Other ways files get into the ingest queue
++++++++++++++++++++++++++++++++++++++++++

There are other ways that files get added to the ingest queue on the server. There is an add_to_ingestqueue.py script which
can be run manually or from cron. Also when the ODB calls the fits server to update a QA state etc, the file is re-added to the
ingest queue after it is modified so that the changes get picked up.

The add_to_ingestqueue.py script takes a --file-re argument that specifies a regular expression - the script will add filenames that match
this regular expression to the ingest queue. This argument can also take various special values which translate to files from the last few days
for example. For more info, run the script with --help::

  [fitsdata@mkofits-lv1 scripts]$ python add_to_ingest_queue.py --help
  Usage: add_to_ingest_queue.py [options]

  Options:
    -h, --help           show this help message and exit
    --file-re=FILE_RE    python regular expression string to select files by.
                         Special values are today, twoday, fourday, tenday
                         twentyday to include only files from today, the last
                         two days, the last four days, or the last 10 days
                         respectively (days counted as UTC days)
    --debug              Increase log level to debug
    --demon              Run as a background demon, do not generate stdout
    --path=PATH          Use given path relative to storage root
    --force              Force re-ingestion of these files unconditionally
    --force_md5          Force checking of file change by md5 not just lastmod
                         date
    --after=AFTER        ingest only after this datetime
    --newfiles=NEWFILES  Only queue files that have been modified in the last N
                         days


If someone modifies a file directly on the dataflow volume (for example to change a header value) then the change will only
get picked up when the file is re-ingested, and simply modifying the file on the filesystem will not trigger that. (Note,
unfortunately we can't even use inotify to trigger this as the NetApp filers that host these filesystems don't support that).

To solve that, add_to_ingestqueue.py is called regularly from cron in order to queue all recent data for reingestion periodically.
More recent data is checked more frequently. Typical crontab entries for this are::

    # Add to ingest queue
    */15 * * * * python /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --file-re=fourday --demon
    * 18 * * * python /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --file-re=twentyday --demon

which will check files from the last 4 days every 15 mines, and files from the last twenty days once per hour (at 18 mintes past).

Also add_to_ingestqueue is called from cron to check for new files in the masks, obslogs and iraf_cals directories::

    2 * * * * python /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --path=masks --demon
    6 * * * * python /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --path=obslogs --demon
    10  * * * * python /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --path=iraf_cals --demon

Note that when a file comes to be ingested, the first thing we do is check the last modification time of the file on disk against
the last modification time of the file when it was last ingested (which is recorded in the database). If the file hasn't been 
modified, the re-ingestion is essentially a null operation, so there's not a huge performance hit from doing this.

Servicing the Ingest Queue
++++++++++++++++++++++++++

The service_ingestqueue.py script takes files from the ingest queue, extracts metadata from them and adds rows to the
database tables to represent that file - we call this ingesting the file into the database. There are generally 
multiple instances of this script running, so that we can work through a backlog of files quicker, both by utilizing
multiple CPU cores on the machine, and by allowing the CPU to process one file while waiting for IO on another for example.
Typically there are two service_ingestqueue jobs running. They are both started or re-started by systemd, with the service
names fits-service_ingest_queue1 and fits-service_ingest_queue2.

You can check the status of these with systemd::

  [phirst@mkofits-lv1 ~]$ sudo systemctl status fits-service_ingest_queue1
  fits-service_ingest_queue1.service - Fits Service Ingest Queue 1
     Loaded: loaded (/etc/systemd/system/fits-service_ingest_queue1.service; enabled)
     Active: active (running) since Wed 2015-12-02 10:27:59 HST; 3 months 15 days ago
   Main PID: 23848 (python)
     CGroup: /system.slice/fits-service_ingest_queue1.service
             └─23848 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq1

  [phirst@mkofits-lv1 ~]$ sudo systemctl status fits-service_ingest_queue2
  fits-service_ingest_queue2.service - Fits Service Ingest Queue 2
     Loaded: loaded (/etc/systemd/system/fits-service_ingest_queue2.service; enabled)
     Active: active (running) since Wed 2015-12-02 10:28:05 HST; 3 months 15 days ago
   Main PID: 23866 (python)
     CGroup: /system.slice/fits-service_ingest_queue2.service
             └─23866 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq2

and by looking at the process list directly::

  [phirst@mkofits-lv1 ~]$ ps aux | fgrep python | fgrep ingest
  fitsdata 23848  2.0  2.1 575956 84520 ?        Ss    2015 3241:45 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq1
  fitsdata 23866  2.0  2.2 576116 86856 ?        Ss    2015 3224:42 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq2

and by tailing their respective log files::

  [phirst@mkofits-lv1 ~]$ tail /data/logs/service_ingest_queue.py-siq1.log
  2016-03-18 15:45:17,309 23848:service_ingest_queue:93 INFO: Ingesting N20160317S0007.fits, (5 in queue)
  2016-03-18 15:45:17,329 23848:service_ingest_queue:93 INFO: Ingesting N20160317S0005.fits, (3 in queue)
  2016-03-18 15:45:17,349 23848:service_ingest_queue:93 INFO: Ingesting N20160317S0003.fits, (1 in queue)
  2016-03-18 15:45:17,368 23848:service_ingest_queue:93 INFO: Ingesting N20160317S0001.fits, (0 in queue)
  2016-03-18 15:45:17,379 23848:service_ingest_queue:86 INFO: Nothing on queue... Waiting
  2016-03-18 15:45:19,384 23848:service_ingest_queue:86 INFO: Nothing on queue... Waiting


If this system is configured to export files to one or more downstream servers - for example the summit servers
export data to the archive this way, then serivice_ingest_queue.py will also add the file to the export queue.

Service the export queue
++++++++++++++++++++++++

service_export_queue.py takes files from the export queue and uploads them to the destination server. MD5 hashes are exchanged
to see if the file is already at the destination with the same md5sum, and the transfer becomes a null operation if it is. MD5s
are also exchanged after the transfer to verify file integrity at the far end.

service_esport_queue.py is run in the same way as service_ingest_queue.py by systemd. There are usually two instances running
with service names fits-service_export_queue1 and fits-service_export_queue2.

You can check the systemd status for them::

  [phirst@mkofits-lv1 ~]$ sudo systemctl status fits-service_export_queue1
  fits-service_export_queue1.service - Fits Service Export Queue 1
     Loaded: loaded (/etc/systemd/system/fits-service_export_queue1.service; enabled)
     Active: active (running) since Tue 2015-12-01 14:08:03 HST; 3 months 16 days ago
   Main PID: 2152 (python)
     CGroup: /system.slice/fits-service_export_queue1.service
             └─2152 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_export_queue.py --demon --lockfile --name=seq1

  [phirst@mkofits-lv1 ~]$ sudo systemctl status fits-service_export_queue2
  fits-service_export_queue2.service - Fits Service Export Queue 2
     Loaded: loaded (/etc/systemd/system/fits-service_export_queue2.service; enabled)
     Active: active (running) since Tue 2015-12-01 14:09:02 HST; 3 months 16 days ago
   Main PID: 2226 (python)
     CGroup: /system.slice/fits-service_export_queue2.service
             └─2226 /usr/bin/python /opt/FitsStorage/fits_storage/scripts/service_export_queue.py --demon --lockfile --name=seq2

and tail their log files::

  [phirst@mkofits-lv1 ~]$ tail /data/logs/service_export_queue.py-seq2.log
  2016-03-18 16:59:30,757 2152:service_export_queue:85 INFO: Nothing on Queue... Waiting
  2016-03-18 16:59:32,762 2152:service_export_queue:85 INFO: Nothing on Queue... Waiting
  2016-03-18 16:59:34,773 2152:service_export_queue:92 INFO: Exporting N20160319S0088.fits, (0 in queue)
  2016-03-18 16:59:36,302 2152:exportqueue:154 INFO: Transferring file N20160319S0088.fits.bz2 to destination https://archive.gemini.edu
  2016-03-18 16:59:38,331 2152:service_export_queue:85 INFO: Nothing on Queue... Waiting
  2016-03-18 16:59:40,345 2152:service_export_queue:85 INFO: Nothing on Queue... Waiting

Dataflow Monitoring and Troubleshooting
=======================================

Each server provides a queue status page that shows the status of the queues on that server. This is located at the /queuestatus
URL on each server. You must be logged in with an account with staff access in order to access it. There are 4 tabs
on the page, one for each queue. The page self refreshes periodically. Each tab heading shows the total number of items
on that queue, and the number that are in an error state. In each tab there are two columns, one showing queue entries
waiting for processing and the other showing those where processing ended in an error. Only the first N entries are shown.
Clicking on the filename in the Errors column shows the python exception and back-trace of the error.

