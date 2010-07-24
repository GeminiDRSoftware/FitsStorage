import urllib2
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--emailto", action="store", dest="toaddr", default="gnda@gemini.edu", help="Email Address to send to")
parser.add_option("--emailfrom", action="store", dest="fromaddr", default="fitsdata@gemini.edu", help="Email Address to send from")
parser.add_option("--replyto", action="store", dest="replyto", default="gnda@gemini.edu", help="Set a Reply-To email header")
(options, args) = parser.parse_args()

# Work out the date range to query
utcnow = datetime.datetime.utcnow()
delta=datetime.timedelta(days=7)
utcthen = utcnow - delta
daterange="%s-%s" % (utcthen.date().strftime("%Y%m%d"), utcnow.date().strftime("%Y%m%d"))

url = "http://fits/calibrations/GMOS/Win/%s/arc/warnings" % daterange

f = urllib2.urlopen(url)
html = f.read()
f.close()

cremissing = re.compile('Counted (\d*) potential missing Calibrations')
crewarning = re.compile('Query generated (\d*) warnings')

warnings = int(crewarning.search(html).group(1))
missing = int(cremissing.search(html).group(1))

mailhost = "smtp.gemini.edu"

if(missing==0):
  subject = "No missing calibrations this week. Yay!"
else:
  subject = "MISSING CALIBRATIONS: %d missing arcs" % missing

msg = MIMEMultipart()

text = "Calibration Check: %d missing, %d warnings.\n\n%s" % (missing, warnings, url)

part1 = MIMEText(text, 'plain')
part2 = MIMEText(html, 'html')

msg['Subject'] = subject
msg['From'] = options.fromaddr
msg['To'] = options.toaddr
msg['Reply-To'] = options.replyto

msg.attach(part1)
msg.attach(part2)

smtp = smtplib.SMTP("smtp.gemini.edu")
smtp.sendmail(options.fromaddr, [options.toaddr], msg.as_string())
smtp.quit()
