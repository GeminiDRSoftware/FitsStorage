Software Upgrades
=================

Software upgrades have become a bit more complex than they once were, as there is now information in the database that needs to be transferred to the
new installation, so we can't just re-start from an empty database each time. Of course, the database schema may have changed slightly in the upgrade,
in which case we can't just keep using the existing database.

So in practice you will need to create a new empty database using the new software version, and then use a database backup dump from the previous
deployed server to restore certain tables into the new database. If the schema for any of those tables has changed you'll need to figure out a way
to handle this.

More than liklely you are going to rebuild the main data file tables (ie file, diskfile, header, the instrument specific tables) from scratch using 
the new software version, as that's likely where the schema updates have been. However, some of the other tables will need to be transfered from the 
previous version. In general (think about specific cases each time you do an upgrade), the following will need transferring:

On the archive server:

* archiveuser
* userprogram
* notification
* glacier
* usagelog
* querylog
* downloadlog
* filedownloadlog
* fileuploadlog
* (and possibly some or all from the summit fits server list below)

On the summit fits servers:

* qametriciq
* qametricpe
* qametricsb
* qametriczp
* qareport
* (and possibly some or all from the archive server list above)

Note that it's also best to restore the corresponding _id_seq sequence with each table, otherwise you'll need to manually update the fresh
id sequence to start from where the restored table leaves off.


Database Rebuilds
+++++++++++++++++

Generally, it's a case of an unconstrained add_to_ingestqueue.py followed by several long running service_ingest_queue.py threads. You will
want to run all these with --demon so that if you loose network to the machine it will keep going.

The service_ingest_queue jobs will likely want some or all of the following options: --fast-rebuild --make-previews --name=NAME --lockfile --empty

In the archive case I normally run a cloud of say 200 machines all running service_ingest_queue jobs to rebuild the database. See notes on this later.

On the archive, you will also need to rebuild the calcache table after the ingest completes. Run as many jobs as you have CPUs on the machine - since 
most of the CPU load for this on on the database server itself, a cloud of clients doesn't help with this. If you're in a hurry, one option is to
create an AWS host with lots of CPUs (say 32 cores) and run this there - you'll need to transfer the database over and back afterwards. 
service_calcache_queue.py --demon --fast-rebuild --name=NAME --lockfile --empty

Historic data on the summit machines
------------------------------------

A use case that we didn't really envision that has arrisen is people wanting to search qametrics for data that is no longer on the summit dataflow
volumes. For the qametric search to work, the data has to be in the database. So the simplest solution to this is that when you do a new summit
release, start with the database backup from the same release on the archvie server. After you restore it to the summit server database do a
large rollcall.py run (or alternatively just update diskfile to set present = Flase on everything) followed by the usual add_to_ingestqueue and 
service_ingest_queue runs.

AWS spot instance notes
-----------------------

Spot instances are were you bid on unused CPU resources in the AWS cloud. It's a cheap way to get hundreds of CPUs to speed up large rebuilds on 
the archive. Some notes:

* Simplest approach is m3.medium hosts (1 CPU, 3.75GB, 4GB SSD). Configure the SSD as swap as a few of the preview builds require large amounts of ram.
* 8GB root filesystem is adequate. Configure it to delete the root fs on termination.
* Bid just below the on-demand price to ensure they run.
* Instantiate one on demand, do the install, configure it to access the database and test that it runs properly.
* Note, will need to configure the main database server to accept remote connections. Note, use the AWS internal IP addresses.
* Set it up so that it starts processing on boot
* Shut it down and make an AMI of it - select instance, create image.
* Put in spot instance bids to use the new AMI.
* Don't forget to go in and terminate them when it's done.

AWS server configuration
++++++++++++++++++++++++

We have an M3.xlarge (4 CPU, 16GB ram, 2x40GB SSD) 3-year all-paid-upfront reserved instance started 4-May-2015, that expires 4-May-2018. 
Normally we wouldn't do a 3 year instance reservation, as you're tied to that server generation for the duration - however the financial
situation at the time meant that paying up front for a 3 year term was advantageous. Of course AWS released the M4 generation systems shortly
after that, but the reserved instance we have is M3 and can't be converted to M4. This isn't really a big deal, but the storage setup to
use the high performance storage on the M3 instance is a little quirky.

M3 class server storage
+++++++++++++++++++++++

The M3.xlarge server comes with 2 high performance 40GB SSDs. The M3 servers are not (currently) optimized for high performance access to 
EBS, so the SSDs are the fastest storage available, which makes them a good option for the database backend storage. But they're so called 
instance stores - these will survive a server reboot intact but if your VM is moved to different physical hardware, or the instance is terminated
rather than stopped then their contents will be lost, which makes them unattractive as the database backend store. Also for our database we need
about 60 GB, which is bigger than either drive.

To deal with all this, we have the following setup. Alongside the two 40GB SSDs are two corresponding EBS volumes. Each SSD is raid mirrores with
an EBS volume using the linux software raid system. The raid is configured with the EBS volume in write-behind and write-mostly modes. This means
that we get the performance of the SSD but we are constantly mirroring the data to EBS. These two raid mirrors are then combined into one logical
volume using lvm2. If we loose the "ephemeral" SSD contents, then when the machine is back up the database should work fine using the EBS volumnes. 
We simply add the new SSD volumes to their respecive raid mirrirs and the raid system will copy the data from EBS to the SSD while the system is live,
and we'll have the SSD performance for data once it's copied to the SSD.

While this is a good solution for now, it's added risk for disaster recovery due to the complexity of the arrangement. If the M3 servers ever
get the high performance access to EBS we should probably convert to that. M4 just uses EBS and does away with the SSDs anyway. Also at some
point (not far away actually), the database will outgrow the space available on the SSDs, so we'll be forced to EBS at that point anyway.
raid mirrors and the raid system will re-populate them from EBS
