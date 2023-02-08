Introduction
************

Very Brief System Overview
==========================

Each Fits Storage system consists of a linux host (currently CentOS 7) running the PostgreSQL database and the 
apache web server. The internal summit systems are deployed on VMs provided by ISG, the GOA runs on an AWS EC2 instance. 

The FitsStorage software executes under the username fitsdata, except when it is invoked by apache when it runs under 
the apache username. All the cron and systemd jobs run under fitsdata, and the database is owned by that userid. There are 
postgres roles for fitsdata and apache for database access.

You will need a personal log in to the machine in order to be able to do anything. You’ll need sudo for most 
troubleshooting tasks. The fitsdata account does NOT have sudo.

There is a fitsadmin@gemini.edu mail alias that should expand to the fits admin people. This address is used as the 
account owner with AWS for example.

The summit machines use the summit dataflow NFS volume for their FITS data store, the GOA uses AWS S3.

All state is maintained in the database, and responses to all web queries are made by querying the database. 
The only times the system looks at the actual FITS files are to populate the database when the file is ingested, 
or to supply the FITS file to a user who requests to download it.

If you’re using this document, please tread very carefully. Almost certainly you will not need to do anything drastic 
to the machine or the database, and if you think you do, then please be **very** sure of your self before proceeding, 
you could make things **A LOT** worse...

Operations Servers
==================

There are three Fits Storage deployments in use operationally. There are others used to development and testing.
For the Summit Servers, there are CNAME entries (aliases) in the Gemini DNS that point canonical names at the actual 
current operational machine. For the GOA there is an entry in the Gemini DNS that points at an Elasic IP address
(52.24.55.47) pseudo-statically allocated to our AWS account for the archive machine.

All users should be referring to the servers with these canonical names, not using the actual hostname of the machine, 
as that will likely change when we update to a new version, which we do by deploying a new server running the new version 
of the code on which we prepare the new database etc, then we simply switch that to be the active server by swapping the 
DNS alias to point to it for the summit servers, or by re-assigning the elastic IP address to the new server for the GOA.

+---------------------+---------------------------------------+
|Canonical Name       | Description                           |
+=====================+=======================================+
|fits.hi.gemini.edu   | MKO (Gemini North) Summit Fits Server |
+---------------------+---------------------------------------+
|fits.cl.gemini.edu   | CPO (Gemini South) Summit Fits Server |
+---------------------+---------------------------------------+
|archive.gemini.edu   | Gemini Obsevatory Archive Server      |
+---------------------+---------------------------------------+

Fits Storage Queues
===================

Many of the tasks that the Fits
Storage software does take some time to execute - for example parsing the headers out of a file and inserting
details in the database takes say about half a second, building a preview image can take several seconds for a
large image, and a complex calibration association can take a second or so on a large database. Also some of these
tasks can be quite CPU / memory and storage bandwidth intensive, and the system may receive a request to carry out
these operations on many thousands of files.

So, what happens is that these requests are added to queues (which are database tables internally). For each queue,
there can be a zero or more instances of the appropriate queue service task running. These tasks look at the queue,
find the highest priority item on the queue that is not being worked on, and work on it. Specifically, they mark the
entry in the queue table as in progress, then work on it, then when they've finished processing that item they remove
it from the queue. The operation of finding the highest priority item and marking it as in progress is done in an
atominc select-for-update manner with suitable locking to prevent multiple queue service jobs from attempting to work
on the same item.

Generally, "highest priority" for the queues is defined as "most recent data", so for example new data files will
take priority over old files from tonight that have been updated, and those in turn will take priority over files
from previous nights that have been updated.

If no jobs are running to service a particular queue, but items are being added to that queue, they'll happily just
build up without being processed until the queue service job gets started. If the queue is empty, the queue service
jobs will sleep for 5 seconds before polling the queue.

There are 4 queues in the Fits Storage system, though some of them may not be used on some systems:

* Ingest Queue - for files to be ingested into the database.
* Export Queue - for files to be exported to another system, typically used by the summit server to export data to the archive
* Preview Queue - for files that need preview images building (only used on the archive)
* CalCache Queue - for files that need adding to the calibration association cache (only used on the archive)

The jobs that service these queues are all run as systemd services and so should start automatically on system boot, and
should get automatically restarted if the die for some reason. Note however that systemd will stop trying to start a process
if it repeatedly crashes as soon as it's started.

Script Log Files
================

There are a number of python scripts that run as part of the system. These can be run manually, but for routine operations
they are usually invoked either by cron or by systemd. All of these scripts generate log files in the Fits Storage log directory,
which is usually /data/logs. In some cases there will be multiple instances of each script running; in that case it is usuall that the 
--name argument is given to each on startup with a different value. This is then used in the log file name to keep the output from the
different instances separate. For example it is common to run two or more instances of the service_ingest_queue.py script. Typically with names
siq1, siq2 etc. The log files for these are then service_ingest_queue.py-siq1.log, service_ingest_queue.py-siq2.log, etc. 

Database Log Entries
====================

Every query to the web interface results in a log entry in the database logging tables. These can be queried and viewed at the
/usagereport URL on the server. You will need to be logged in as a user with staff access in order to access the usage log. This
/usagelog feature is the easiest way to investigate reports of errors. You can search by various fields to find the events in question, for 
example username if the user was logged in, or IP address if you know it. Note that of course users accessing the archive system at amazon
from within at NAT network such as at Gemini will appear to come from the external IP address of their firewall, which will be different
from their local machine internal IP address. Obviously, you can also search by date/time and by the URL feature they were accessing. Finally,
and importantly, you can search the logs by HTTP status code, so if you have a user seeing "500 Internal Server Error" you can find those
quickly by searching for status 500. 

In the Usage Log results table, you can click on the ID of the log entry to see further details. If the code generated a python exception
while processing the request, the exception and backtrace will be stored in the Notes section of the log entry for that request.

