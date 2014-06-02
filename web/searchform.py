"""
This is the searchform module
"""

# This will only work with apache
from mod_python import apache, util

from web.selection import getselection

from fits_storage_config import fits_aux_datadir
import os

# Load the html text into strings
with open(os.path.join(fits_aux_datadir, "titlebar.html")) as f:
    titlebar_html = f.read()

# Load the form text into strings
with open(os.path.join(fits_aux_datadir, "form.html")) as f:
    form_html = f.read()

def searchform(req, things):
   """
   Generate the searchform html and handle the form submit.
   """

   # How (we think) this (will) all work(s)
   # User gets/posts the url, may or may not have selection criteria on it
   # We parse the url, and create an initial selection dictionary (which may or may not be empty)
   # We parse the formdata and modify the selection dictionary if there was any
   # If there was formdata:
   #    Build a URL from the selection dictionary
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

   # grab the string version of things before getselection() as that modifies the list.
   thing_string = '/' + '/'.join(things)
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
   req.write('<input type="hidden" id="things" name="things" value="%s">' % thing_string)
   req.write('<div class="page">')
   req.write(form_html)
   req.write('<div class="searchresults">')
   
   if(formdata):
     # Populate selection dictionary with values from form input
     for key in formdata.keys():
         if key == 'program_id':
             selection[key] = 'progid=' + formdata[key].value   
         else:
             selection[key] = formdata[key].value
   
   req.write('<h1>Search results go here</h1>')
   req.write('<p>%s</p>' % selection)
   req.write('<p>%s</p>' % formdata)
   req.write('</div>')
   req.write('</div>')
   req.write('</body></html>')

   return apache.OK
