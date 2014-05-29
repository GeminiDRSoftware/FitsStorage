"""
This is the searchform module
"""

# This will only work with apache
from mod_python import apache
from mod_python import util

from web.selection import getselection

# Load the html text into strings
f = open("/opt/FitsStorage/htmldocroot/htmldocs/titlebar.html")
titlebar_html = f.read()
f.close()

# Load the form text into strings
f = open("/opt/FitsStorage/htmldocroot/htmldocs/form.html")
form_html = f.read()
f.close()

def searchform(req, things):
   """
   Generate the searchform html
   """
   selection = getselection(things)
   formdata = util.FieldStorage(req)

   req.content_type = "text/html"
   req.write('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd"><html><head>')
   req.write('<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>')
   req.write('<script src="/htmldocs/titlebar.js"></script>')
   req.write('<script src="/htmldocs/form.js"></script>')
   req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/whoami.css">')
   req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/titlebar.css">')
   req.write('<link rel="stylesheet" type="text/css" href="/htmldocs/form.css">')
   req.write('<title>Gemini Archive Search Form</title></head><body>')

   req.write(titlebar_html)
   req.write('<div class="page">')
   req.write(form_html)

   req.write('<div class="searchresults">')
   if(formdata):
     # This is where we handle what came back from the form
     req.write('<h1>Search results go here</h1>')
     req.write('<p>%s</p>' % formdata)
   if(selection):
     req.write('<h1>Selection</h1>')
     req.write('<p>%s</p>' % selection)

   req.write('</div>')

   req.write('</div>')
   

   req.write('</body></html>')

   return apache.OK
