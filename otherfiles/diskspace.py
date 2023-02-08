#!/usr/bin/env python

import os
import smtplib

mailfrom = 'phirst@gemini.edu'
mailto = ['phirst@gemini.edu']
#mailto = ['phirst@gemini.edu']

disks = { "/sci/dataflow": 90, "/net/mko-nfs/sci/pipestore": 20, '/sci/dhs': 50}

low = 0

email = "Disk Space Checking script says:\n\n"

for disk in disks.keys():
  # Do a chdir to kick the automounter
  os.chdir(disk)
  s = os.statvfs(disk)
  gbavail = s.f_bsize * s.f_bavail / (1024 * 1024 * 1024)
  if(gbavail < disks[disk]):
    low+=1
    email += "Disk %s is LOW ON SPACE - Free space = %.2f GB, should be at least %.2f GB\n\n" % (disk, gbavail, disks[disk])
  else:
    email += "Disk %38s is fine: %.2f GB free\n\n" % (disk, gbavail)

if (low):
  subject = "Urgent: LOW DISK SPACE"
  msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (mailfrom, ", ".join(mailto), subject, email)

  server = smtplib.SMTP('localhost')
  server.sendmail(mailfrom, mailto, msg)
  server.quit()

