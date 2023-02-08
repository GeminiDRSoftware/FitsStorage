Basic System Functionality (aka HELP, it’s dead!)
=================================================

First up, the machine obviously has to be up at the Linus OS level. If you can’t even ssh in, then you’ll need 
assistance from ISG for the summit servers, or you’ll need to get on to the AWS web site EC2 management 
console for the GOA.

GOA: Go to https://aws.amazon.com and login as fitsadmin. Go into EC2. There may be a few instances there if 
there’s any testing or development work going on at the moment, some may be stopped. The main archive instance should 
have a name like ‘archive_2016-1 for the 2016-1 version production server) Is the archive instance up and running? 
If it claims to be OK at AWS but you can’t log in, maybe the server is actually fine and there’s issue with internet 
connectivity from Gemini? If you’re sure that’s not the problem, you can reboot the EC2 instance from there, 
though that is not something you should do lightly. If the instance is showing as Stopped, then go ahead and start it. 
Under no circumstances should you terminate the EC2 instance. See AWS disaster recovery later in this document if the 
EC2 instance is missing or dead. The archive instance should have the Elastic IP address 52.24.55.47 bound to it, which 
is in the Gemini DNS as archive.gemini.edu

OK, assuming that the linux host is up and that you can ssh in, but the web service is non functional or having severe issues. 
If the service seems to be working OK, but it’s something like you’re missing new files or calibration associations or something, 
then go ahead and sanity check, but probably you don’t need to restart the services or anything.

First up so a sanity check on the system - none of the filesystems should be full, and CPU load should generally be low. 
If this is not the case, or something else is messed up, it may be a good idea to shut down the http service while you troubleshoot - 
this prevents external users from hitting the system which may be compounding the issue. To shutdown the http service: sudo systemctl stop httpd. 
Remember to start it back up when you’re done!

If things are really messed up, or the /data filesystem is full, you can also shut down the postgres database service at this time 
with sudo systemctl stop postgres. The postgres data directory is on /data, so if that’s full, chances are the database is unhappy. 
Again, remember to restart it (before httpd) later.

With these services down, you should be able to free up disk space as required, or kill (possibly sudo kill -9) any process that is 
bogging down the CPU. Once things appear sane, first up restart postgres with sudo systemctl start postgres. The database should come up 
OK (sudo systemctl status postgres should show it running). 

If the postgres service is up, then you can do a quick sanity check on the database by logging in to the machine as fitsdata and starting 
the psql postgres interactive SQL prompt. See Database Sanity Checks later

Once the database seems OK, restart the http service with sudo systemctl restart httpd.

If you’re seeing CPU load issues, then if the problem process is a python process owned by fitsdata then almost certainly it’s 
being started by cron from the fitsdata account or it's a task being started by systemd. You could comment it out in the crontab 
or stop it at the systemd level while you troubleshoot.

