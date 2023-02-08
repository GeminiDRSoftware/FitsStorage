# Create a backup disk

1. EC2 dashboard - EBS - Volumes
2. Create volume
3. Give it a name (edit name column)
4. Select new volume then actions - attach volume
5. click in instance id field, start typing, select
6. set device name (or not - gets reassigned anyway...)
7. click attach
8. inside instance, dmesg and inspect /proc/partitions to find new device name
9. mke2fs -t ext4 -m 1 /dev/xvdj
10. mkdir /backups
11. mount /dev/xvdj /backups

# Doing backups
1. cd /backups
2. mkdir YYYYMMDD
3. cd YYYYMMDD
4. lvdisplay
5. lvcreate -L 1G -n backup -s /dev/ssd_ebs/pgdata 
6. dump -0 -f pgdata.dump.0 /dev/ssd_ebs/backup
7. lvremove /dev/ssd_ebs/backup
8. lvcreate -L1G -n backup -s /dev/ebs/data 
9. dump -0 -f data.dump.0 /dev/ebs/backup 
10. lvremove /dev/ebs/backup
11. dump -0au -f root.dump.0 /
12. umount /backups
13. in EC2 console, detach it from instance and make a snapshot of it

# Snapshot root disk
1. EC2 dashboard - EBS - Volumes
2. Select root disk
3. Reduce activity on volume as much as possible
4. Actions - create snapshot

# Create image of entire system
1. EC2 dashboard, select instance
2. actions - create image
3. add instance store to instance volumes
    - warning, this will reboot the instance so it can shapshot while it's down
5. click go
    - instance will shut down, wait for snapshot, then reboot
    - takes a while longer to finalize the AMI though

# To boot new instance from AMI
1. EC2 console - Instances
2. Launch Instance
3. My AMIs
4. select AMI
5. set security group
6. add instance storage if applicable
7. launch
8. ssh to it's public IP address
9. df -hl
10. lvdisplay
11. cat /proc/mdstat

# Using spot instances to accelerate a rebuild (untested)
1. Build primary server
2. Allow remote DB access on primary server
3. Snapshot primary server
4. Launch on demand instance from snapshot
5. clean up unneeded filesystems, raid, lvm etc
6. configure to access remote DB
7. configure to start ingest jobs
8. reboot and check it comes up and processes
9. snapshot on demand server
10. Check spot instance pricing. Bid high so they don't get terminated.
11. launch spot instances from this snapshot.
