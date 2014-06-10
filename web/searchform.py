"""
This is the searchform module
"""

# This will only work with apache
from mod_python import apache, util

from web.selection import getselection, selection_to_URL

from fits_storage_config import fits_aux_datadir
import os

from gemini_metadata_utils import GeminiDataLabel, GeminiObservation, GeminiProject, gemini_date, gemini_daterange

# Load the titlebar html text into strings
with open(os.path.join(fits_aux_datadir, "titlebar.html")) as f:
    titlebar_html = f.read()

# Load the form html text into strings
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

   if(formdata):
       # Populate selection dictionary with values from form input
       updateselection(formdata, selection)
       # builds URL, clears formdata, refreshes page with updated selection from form
       urlstring = selection_to_URL(selection)
       formdata.clear()
       util.redirect(req, '/searchform' + urlstring)       

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
   req.write('<form class="searchform" action="/searchform" method="POST">')
   
   form_html_updt = updateform(form_html, selection)
   req.write(form_html_updt)
   selectionstring = selection_to_URL(selection)

   if(selection):
       req.write('<input type="hidden" id="url" value="%s">' % selectionstring)
   
   req.write('</form>')
   req.write('<hr noshade>')
   # Uncomment this for form processing selection debugging...
   # req.write('<p>selection: %s</p>' % selection)
   req.write('<div id="searchresults" class="searchresults">')
   req.write('<span id="notloading"><P>Set at least one search criteria above to search for data</P></span>')
   req.write('<span id="loading" style="display:none"><p><img src="/htmldocs/ajax-loading.gif">  Loading...</p></span>')
   req.write('</div>')
   req.write('</div>')
   req.write('</body></html>')

   return apache.OK

def updateform(html, selection):
    """
    Receives html page as a string and updates it according to URL selection values
    Pre-populates input fields with said selection values
    """
    for key in selection.keys():
        if key in ['program_id', 'observation_id', 'data_label']:
            html = html.replace('name="program_id"', 'name="program_id" value="%s"' % selection[key])
        elif key in ['date', 'daterange']:
            # If there is a date and a daterange, only use the date part
            if('date' in selection.keys() and 'daterange' in selection.keys()):
                key = 'date'
            html = html.replace('name="date"', 'name="date" value="%s"' % selection[key])
        elif key in ['target_name', 'ra', 'dec', 'search_rad', 'cntrl_wvlngth']:
            html = html.replace('name="%s"' % key, 'name=%s value="%s"' % (key, selection[key]))
        elif key in ['inst', 'observation_class', 'observation_type', 'filter', 'resolver', 'binning', 'disperser', 'mask',]:
            html = html.replace('value="%s"' % selection[key], 'value="%s" selected' % selection[key])
        elif key == 'spectroscopy':
            if (selection[key] is True):
                html = html.replace('value="spectroscopy"', 'value="spectroscopy" selected')
            else:
                html = html.replace('value="imaging"', 'value="imaging" selected')
        else:
            html = html.replace('value="%s"' % selection[key], 'name="%s" checked' % key)
    
    return html

def updateselection(formdata, selection):
    """
    Updates the selection dictionary with user input values in formdata
    Handles many different specific cases
    """
    # Populate selection dictionary with values from form input
    for key in formdata.keys():
        value = formdata[key].value
        if key == 'program_id':
            # if the string starts with progid= then trim that off
            if value[:7]=='progid=':
                value = value[7:]
                # accepts program id along with observation id and data label for program_id input
                # see if it is an obsid or data label, otherwise treat as program id
                go = GeminiObservation(value)
                dl = GeminiDataLabel(value)

                if(go.observation_id):
                    selection['observation_id'] = value
                elif(dl.datalabel):
                    selection['data_label'] = value
                else:
                    selection['program_id'] = value
        elif key == 'date':
            # removes spaces from daterange queries
            value = value.replace(' ', '')
            selection[key] = value
        elif key in ['ra', 'dec']:
            # formats RA and dec values appropriately, converts to decimal degrees if necessary
            selection[key] = value
            value = value.replace(' ', '')
            rangesplit = str.split(value, ',')
            selectionstrings = []
            for stringval in rangesplit:
                if ':' in stringval:
                    # converts RA values to decimal degrees
                    ra_strings = str.split(stringval, ':')
                    ra_vals = []
                    for num in ra_strings:
                        num = float(num)
                        ra_vals.append(num)
                    if len(ra_vals) == 3 and key == 'ra':
                        hrs = (ra_vals[0] / 24) * 360
                        mins = (ra_vals[1] / 1440) 
                        sec = (ra_vals[2] / 86400)
                        degs = hrs + mins + sec
                        selectionstrings.append(degs)
                    elif len(ra_vals) == 3 and key == 'dec':
                        # converts dec values to decimal degrees
                        degs = ra_vals[0]
                        mins = (ra_vals[1] / 1440) 
                        sec = (ra_vals[2] / 86400)
                        if degs >= 0:
                            degs +=  mins + sec
                        else: 
                            degs -= mins + sec
                        selectionstrings.append(degs)
                    else:
                        for val in ra_strings:
                            selectionstrings.append(val)
                else:
                   selectionstrings.append(stringval)
            selection[key] = '%s,%s' % ("{0:.3f}".format(selectionstrings[0]), "{0:.3f}".format(selectionstrings[1]))
        else:
            selection[key] = value

