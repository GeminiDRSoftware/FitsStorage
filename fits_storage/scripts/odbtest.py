import urllib.request, urllib.parse, urllib.error
import re


if __name__ == "__main__":
    program_id = 'GN-2014A-Q-1'
    password='xxxxx'

    url = "https://gnodb.gemini.edu:8443/auth?id=%s&password=%s" % (program_id, password)
    u = urllib.request.urlopen(url)
    html = u.read()
    u.close
    print("reply: %s" % html)
