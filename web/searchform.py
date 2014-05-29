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
   Generate the searchform html and handle the form submit.
   """

   # How (we think) this (will) all work(s)
   # User gets/posts the url, may or may not have selection criteria on it
   # We parse the url, and create an initial selection dictionary (which may or may not be empty)
   # We parse the formdata and modify the selection dictionary if there was any
   # If there was formdata:
   #    Build a URL from the form data
   #    Clear the formdata from the request object
   #    Re-direct the user to the new URL (without and formdata)
   # Pre-populate the form fields with what is now in our selection dictionary
   #    a: by stuffing hidden input elements in the html which jquery uses to modify
   #       the values in the actual input fields 
   # or b: by modifying the form html server side before we send it out
   # Add a hidden input element telling jquery whether to ajax in search results
   # Send out the form html
   # jquery will ajax in results if applicable
   # User messes with input fields
   # User hits submit - back to top

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
