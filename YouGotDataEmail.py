import urllib2
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--emailfrom", action="store", dest="fromaddr", default="fitsdata@gemini.edu", help="Email Address to send from")
parser.add_option("--replyto", action="store", dest="replyto", default="gnda@gemini.edu", help="Set a Reply-To email header")
(options, args) = parser.parse_args()

mailhost = "smtp.gemini.edu"
cre = re.compile('\.fits')

# The project / email list. Hardcoded for now.
list={'GN-2010B-SV-142': 'phirst@gemini.edu'}

for projectid in list.keys():

  url = "http://fits/summary/today/%s" % projectid

  f = urllib2.urlopen(url)
  html = f.read()
  f.close()

  match = cre.search(html)

  if(match):

    subject = "New Data for %s" % projectid

    msg = MIMEMultipart()

    text = "New data has been taken for project ID %s\n\n%s" % (projectid, url)

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    msg['Subject'] = subject
    msg['From'] = options.fromaddr
    msg['To'] = list[projectid]
    msg['Reply-To'] = options.replyto

    msg.attach(part1)
    msg.attach(part2)

    smtp = smtplib.SMTP(mailhost)
    smtp.sendmail(options.fromaddr, list[projectid], msg.as_string())
    smtp.quit()
