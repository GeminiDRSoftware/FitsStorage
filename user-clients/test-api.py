import urllib2
import json

gemini_api_authorization = 'f0a49ab56f80da436b59e1d8f20067f4'

url = 'http://cpofits-lv1new/update_headers'
filename = 'S20151110S0001.fits'
state = "Pass"

d = [{"filename": filename, "values": {"qa_state": state}}]

request = urllib2.Request(url, data=json.dumps(d))
request.add_header('Cookie', 'gemini_api_authorization=%s' % gemini_api_authorization)

u = urllib2.urlopen(request, timeout=30)
response = u.read()
u.close()
http_status = u.getcode()

print "Status: %d" % http_status
print "Response: %s" % response
