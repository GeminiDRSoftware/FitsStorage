Support Notes
=============

This document is for notes on supporting the Archive and the FITS Servers.
It's focused on triage and fixing specific issues.

Website Issues
--------------

Website Not Responding
^^^^^^^^^^^^^^^^^^^^^^

If the website is not responding, first try to ssh
into the host.  If you can't ssh in, the machine may
need to be restarted.  At that point, reach out to IT.

If you can `ssh` in, we can look for other issues.

Is The Webserver Running?
"""""""""""""""""""""""""

To check if the webserver is running, I use `ps`

.. code:: bash

   [ooberdorf@hbffits-lv4 ~]$ ps -Aef | grep http
   ooberdo+  3257  3225  0 13:36 pts/0    00:00:00 grep --color=auto http
   root     16441     1  0 Oct26 ?        00:00:06 httpd (mod_wsgi-express) -f /opt/modwsgi-default/httpd.conf -DMOD_WSGI_ACCESS_LOG -DMOD_WSGI_WITH_PYTHON_PATH -DMOD_WSGI_MPM_ENABLE_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_WORKER_MODULE -DMOD_WSGI_MPM_EXISTS_PREFORK_MODULE -k start
   apache   16442 16441  0 Oct26 ?        00:00:16 (wsgi:localhost:80:0)    -f /opt/modwsgi-default/httpd.conf -DMOD_WSGI_ACCESS_LOG -DMOD_WSGI_WITH_PYTHON_PATH -DMOD_WSGI_MPM_ENABLE_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_WORKER_MODULE -DMOD_WSGI_MPM_EXISTS_PREFORK_MODULE -k start
   apache   16443 16441  0 Oct26 ?        00:00:00 httpd (mod_wsgi-express) -f /opt/modwsgi-default/httpd.conf -DMOD_WSGI_ACCESS_LOG -DMOD_WSGI_WITH_PYTHON_PATH -DMOD_WSGI_MPM_ENABLE_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_EVENT_MODULE -DMOD_WSGI_MPM_EXISTS_WORKER_MODULE -DMOD_WSGI_MPM_EXISTS_PREFORK_MODULE -k start

If you don't see httpd worker processes like this, you can (re)start the server.
Even if the server is running, you might try doing a restart anyway in case HTTPD
ran out of workers.

.. code:: bash

   sudo systemctl restart fits-httpd


Check the API Server
""""""""""""""""""""

There is a second server process that the HTTPD website uses for some operations.
This is where requests to, for instance, update header keywords get handled.  So,
depending on what operation was failing, it may be the API Server that is having
issues instead of the HTTPD Server.

Triage is similar.  Look for `api_backend.py`:

.. code:: bash

   [ooberdorf@hbffits-lv4 modwsgi-default]$ ps -Aef | grep api
   ooberdo+  3327  3225  0 13:48 pts/0    00:00:00 grep --color=auto api
   fitsdata 16281     1  0 Oct26 ?        00:00:31 /usr/bin/python3 /opt/FitsStorage/fits_storage/scripts/api_backend.py --demon

Restart is similar:

.. code:: bash

   sudo systemctl restart fits-api-backend.service

Check the Logs
""""""""""""""

The logs for the HTTPD server live in `/opt/mod-wsgi`.  The `error_log` is where I check first
for any major issues.  There may be some benign astropy messages which you can ignore.

I also look at access_log.  Here I can see if requests are making it to the HTTPD server.
This has never been an issue, but it is worth checking.

.. code:: bash

   cd /opt/modwsgi-default
   tail -100 error_log
   tail -100 access_log

Check disk space
""""""""""""""""

If one of the server disks fills up, this can cause issues.  You can check with a simple `df`.

.. code:: bash

   df -h

I am most interested in `/`, `/data/`, and `/sci/dataflow/`.  `/` could indicate problems in `/tmp`.
`/data` is used for things like staging decompressed FITS files and database backups.
`/sci/dataflow` is where the datafiles live.  Be cautious of doing an `ls` in the `/sci/dataflow`
folder as it will take a long time to complete.

Of note, some of the filesystems run invisible 'snapshot' folders setup by IT.  In this
case you can delete files but no space is released.  Fun times.

Ingest Issues
-------------

Before chasing down a missing file, it's best to make sure the file is actually missing.
I've had many instances where the user was just doing a search they thought should
return the file and did not.  You can try a quick look at their web query if you have
it, or the most foolproof method is to just look in the database:

.. code:: sql

   select * from Diskfile where filename='<filename>';

If there is a record, see if there's a `canonical` record.

.. code:: sql

   select * from Diskfile where filename='<filename>' and canonical;

If there is a `canonical` record, is it marked as `present`?  If not, the file did
exist on the FITS Server and was later cleared out for space.  The file should be
available on the Archive website still.  The FITS Server only shows search results
for files it still has available in `/sci/dataflow`.

If the file is in the database, but does not appear in search results, try searching
with the *Engineering data* advanced option turned on.  It also may be worth checking
that a header row was properly created.  These are unlikely issues, but you can
check for them while you are in the database.

.. code:: sql

   select h.id, df.filename from Header h, Diskfile df where df.filename='<filename>' and df.canonical and h.diskfile_id=df.id

Once you've verified the file does not exist on the target system's database, you
can move on to diagnosing the cause.

Ingest issues fall broadly into two categories.  One is files not showing up on the FITS Servers
themselves and the other is files not  making it into the Archive website.

FITS Server Ingest
^^^^^^^^^^^^^^^^^^

If a file isn't ingested into the FITS Server, I start by looking if the file was placed in
`/sci/dataflow` at all.  There are various problems that can prevent a file from making it
onto dataflow and that naturally means it won't show up in the webserver search pages.

If you are familiar with the layout, you can do something a bit more targeted, but otherwise
this is a good brute force check (note: find takes a long time!):

.. code:: bash

   ls /sci/dataflow/<filename>
   # if not found...
   find /sci/dataflow -name <filename>

If the file did not show up, the next step depends on where the file comes from.  This can
be a normal file we get from the DHS folder.  It can be a visiting instrument like \'Alopeke
or IGRINS.  It can be GRACES data.

GRACES
""""""

For GRACES, the staff copy it into `/sci/dataflow/graces` before we touch it.  So if it is
not in that folder, you should check with them.  This likely is just an operator who is
not familiar with the procedure on their side.  Another thing to check is the permissions
on the file if it is in `/sci/dataflow/graces`.  Since it is copied in by users it may
have permissions that the `fitsdata` user can't read.

DHS (Regular) Data
""""""""""""""""""

The first thing I check is if the DHS copy job is running

.. code:: bash

   [ooberdorf@mkofits-lv3 ~]$ ps -Aef | grep dhs
   ooberdo+  4909  4629  0 15:30 pts/3    00:00:00 grep --color=auto dhs
   fitsdata 27509     1 11 Aug18 ?        8-04:42:14 /usr/bin/python3 /opt/FitsStorage/fits_storage/scripts/copy_from_dhs.py --debug --demon

If it is, check if the file looks ok.  The DHS job will check for a valid file before
copying it into dataflow.  Opening the file with astrodata or looking for the `TELESCOP`
keyword are typical easy checks.  You can also take a look in the DHS copy job logs:

.. code:: bash

   cd /data/logs
   less copy_from_dhs.py.log.1

If a DHS file was copied to dataflow but it is older than 20 days, it also won't be
picked up by the cronjob that adds these to the ingest queue.  This is rare, and the file
can be manually added to the ingest queue (see General Considerations, below)

Visiting Instruments (\'Alopeke, Zorro, IGRINS)
"""""""""""""""""""""""""""""""""""""""""""""""

These are patched and copied by a separate process.  The files also go into `/sci/dataflow/zorro` (etc.)
in date encoded folders.  This makes it easier to poke around.  The copy process is more
complex than for DHS as files are uncompressed, header keywords are repaired, and then the
file is recompressed and placed in dataflow.  This is another case where I would bunzip a
copy of the data and check if the file opens in astrodata and looks valid.  Otherwise, I
first look for the copy job and there is also a log.

.. code:: bash

   cd /data/logs
   tail copy_from_visiting_instrument.py.log.1

SkyCam
""""""

These files are just in Chile.  The files are copied into `/sci/dataflow/skycam` by
Lindsay.

General Considerations
""""""""""""""""""""""

Regardless of what type of file, once it is placed in dataflow, it will be added to
the ingest queue and entered into the database by the ingest job.  We can check if
that is running.

.. code:: bash

   [ooberdorf@mkofits-lv3 logs]$ ps -Aef | grep ingest
   ooberdo+  5088  4629  0 15:38 pts/3    00:00:00 grep --color=auto ingest
   fitsdata 12288     1  6 Jul22 ?        6-11:24:55 /usr/bin/python3 /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq1
   fitsdata 12299     1  6 Jul22 ?        6-10:20:04 /usr/bin/python3 /opt/FitsStorage/fits_storage/scripts/service_ingest_queue.py --demon --lockfile --name=siq2

We can check for issues in the database:

.. code:: sql

   select * from ingestqueue where filename='<filename>';

If failed is True, you can try setting it to False.  If in_progress is set to True, you
can try setting it to False (I prefer stopping the ingest services, setting in_progress
to False, then starting the services).

It is possible, and again rare, for the `failed` or `in_progress`
state to allow a second entry in the queue for a file.  In that case,
constraints will refuse to let you update the stuck entry as it
would collide with the clean one.  It's safe to delete one of the
entries in that case (generally the stuck one).

You can look at the `queue_error` table to see what the error message was.

.. code:: sql

   select * from queue_error where filename='<filename>' and queue='INGEST' order by added desc;

Finally, if you want to add a file to the ingest queue to force the service to try
and ingest it again, you can do:

.. code:: bash

   sudo -u fitsdata env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --file-re=N20200804S0091.fits --force

If the file is in a subdirectory, such as \`Alopeke data, I add the path argument and I
prefer to add a logsuffix so errors go a seperate log.  Add `--force` if you want the
file to ingest even if it appears unchanged (for instance, to pick up code changes that
were made for parsing the header).

.. code:: bash

   sudo -u fitsdata env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --logsuffix=alopeke --path=alopeke/20201023 --file-re=N20201023A0021
   sudo -u fitsdata env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 /opt/FitsStorage/fits_storage/scripts/add_to_ingest_queue.py --logsuffix=graces --path=graces --file-re=N20200926G005

Archive Ingest
^^^^^^^^^^^^^^

If a file does not show up on Archive, first check if it has ingested on the appropriate
FITS Server.  If the file is not on `fits.hi.gemini.edu` or `fits.cl.gemini.edu`, then
proceed to triage that as everything must ingest on a FITS Server before it is pushed to
the Archive.

Export Service
""""""""""""""

If a file is on the FITS Server, check if the export service is running on that FITS Server.
This is the job that continuously pushes newly ingested data to the Archive.  There is
normally just one running.

.. code:: bash

   [ooberdorf@mkofits-lv3 ~]$ ps -Aef | grep export
   ooberdo+  2191  2163  0 13:52 pts/0    00:00:00 grep --color=auto export
   fitsdata 31051     1  1 Jun16 ?        2-13:53:27 /usr/bin/python3 /opt/FitsStorage/fits_storage/scripts/service_export_queue.py --demon --lockfile --name=seq1

If the export service is not running, you can (re)start it.

.. code:: bash

   sudo systemctl restart fits-service_export_queue1

You can check the logs for the export queue for issues.

.. code:: bash

   cd /data/logs
   grep <filename> service_export_queue.py-seq?.log*
   tail -20 service_export_queue.py-seq1.log

If you do find errors in the log, this may need followup on the Archive server.
For instance, if it seems like the webservice calls to the Archive to submit
the file are failing.  Otherwise, I will continue on with these further
triage steps:

You can also log into the Postgres Database as `fitsdata` and check the `exportqueue`.

.. code:: bash

   sudo -u fitsdata psql fitsdata

.. code:: sql

   select * from exportqueue where filename='<filename>' order by added desc;

Normally, the file will not be in the `exportqueue`.  If it is, it's possible the
export job simply hasn't reached that file yet.  See if the `failed` column is set.
If so, the queue failed to send the file and won't try again until you clear it.

.. code:: sql

   update exportqueue set failed=False where filename='<filename>';

Another very unlikely possibility is the file marked as `in_progress` but the
`exportqueue` job died or was restarted.  The safest way to do this is to
shut down the export job(s), then update the flag, then start the jobs.
This avoids the possibility that the file was legitimately in progress
and having it two export queues fight over it.

.. code:: bash

   sudo service stop fits-service_export_queue1
   # and any other instances

in Postgres:

.. code:: sql

   update exportqueue set in_progress=False where filename='<filename>';

.. code:: bash

   sudo service start fits-service_export_queue1
   # and any other instances

It is possible, and again rare, for the `failed` or `in_progress`
state to allow a second entry in the queue for a file.  In that case,
constraints will refuse to let you update the stuck entry as it
would collide with the clean one.  It's safe to delete one of the
entries in that case (generally the stuck one).

If the file fails again, you can look at the `queue_error` table.

.. code:: sql

   select * from queue_error where queue='EXPORT' and filename='<filename>' order by date desc limit 5;

If it looks like the problem is on the Archive, this will depend on what
the problem looks like.  If the export webservice requests are unable to
POST to the archive at all, this is the same process as figuring out why
a server is unresponsive from above.  If files are posting to the Archive,
but still not showing up in the web interface there, then we need to look
at the services on `archive.gemini.edu`.


Emails
------

User Notifications
^^^^^^^^^^^^^^^^^^

We regularly send emails to the users when their program data is available.
The list of users and their programs is stored on the FITS Store servers
in Hawaii and in Chile.  The scripts that check daily for emails to send runs
in each of these locations for programs that are local there.  However,
the datafiles are checked for on the Archive server.

You can check that the email job is enabled by looking at the crontab.
It is currently in the cron for the fitsdata user, but if that looks empty
it may have been moved to a root crontab or similar.

Here are the jobs in Hawaii's crontab

.. code::

   0 8 * * *  python3 /opt/FitsStorage/fits_storage/scripts/YouGotDataEmail.py --demon
   0 8 * * *  python3 /opt/FitsStorage/fits_storage/scripts/YouGotDataEmail.py --demon --check

The script will read the rows in the `notification` table.  The selection field is
the query that will be run against the Archive website looking for matches.  The
email notifications will go to all three emails listed in that row.

Notice also the `--check` job.  This job is different but uses mostly the same
logic as the user notifications.  This job, with the `--check`, looks for files with
a CHECK QA state on the local FITS Store.  It does not email all the users, only the
`csemail` indicated in the `notification` table.

If a user isn't receiving an email they expected, and the cron job looks ok, I
first check what notifications exist for the program.

.. code:: sql

   select * from notification where selection like '%<program_id>%'

If you do see a row, check the emails.  Pay particular attention if any of the
emails fields has multiple emails or odd formatting.  This may not have been
parsed as intended by the script.  As these are sourced from ODB, it is better
to make the script smarter than to try and fix the field in the database.  It
will simply get reset again from the ODB on the next load.  You can also check
the Archive to see what the `selection` column returns:

.. code::

   https://archive.gemini.edu/searchform/GN-2019A-FT-211/science

Also consider the notification script runs with a date.  By default, it uses `today` but
you can also pass a `YYYYMMDD` encoded date if you are checking a previous day's results.

.. code::

   https://archive.gemini.edu/searchform/GN-2019A-FT-211/science/today

If all of that looks ok, the next things to check are the logs in `/data/logs/YouGotDataEmail.py.log`
and work with ITOps to check the email server configuration.

OT
--

OT Shows Observations "Not in Public Archive"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, I check if the data are showing up in `archive.gemini.edu` search results.  If not, I
will also search with Engineering data turned on.  It's possible something is causing the archive
to not recognize the data.

Usually, I find the data is actually showing fine on the website.  In that case, check the SSL
certificate on archive.  You can see this by clicking on the 'lock' icon in Chrome just left of
the URL.  Then click on "Certificate".  Then click the "Details" drop down.  Scroll down and
find the "Timestamp" near the bottom.  If this lines up with when the OT stopped seeing the
data, then the OT probably needs a certificate update.  Their host doesn't work well with
Let's Encrypt certificates for some reason and when the certificate automatically updates
every ~3 months it can break this connection.
