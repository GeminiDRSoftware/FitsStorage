Using spot instances to bulk ingest from s3.

Simplest approach would be use m3.medium hosts (1 CPU, 3.75GB, 4GB SSD unused)

Based on arcdev, 8GB root filesystem should be adequate.
lowest available M3.medium spot price typeically 0.0081/hr, on demand is 0.07/hr so bid say 0.05 to make sure we get them.

So, instantiate one ondemand, set it up to just go on boot, then shut it down and snapshot it.
note use --fast_rebuild (aswell as --name, --demon, --lockfile) on service_ingestqueue

Make an AMI from the snapshot. (select instance, create image)
Need to set up the master to allow connections etc
Need to set up the spot client to connect to the master database. Use the internal AWS IP.
Then put in the bid for the spot instances of that AMI
