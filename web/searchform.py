"""
This is the searchform module
"""

# This will only work with apache
from mod_python import apache, util

from web.selection import getselection, selection_to_URL

from fits_storage_config import fits_aux_datadir
import os
import urllib

from gemini_metadata_utils import GeminiDataLabel, GeminiObservation

# Load the titlebar html text into strings
with open(os.path.join(fits_aux_datadir, "titlebar.html")) as f:
    titlebar_html = f.read()

# Load the form html text into strings
with open(os.path.join(fits_aux_datadir, "form.html")) as f:
    form_html = f.read()

def searchform(req, things, orderby):
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

    # Also args to pass on to results page
    args_string = ""
    if (orderby):
        args_string = '?orderby=%s' % orderby[0]
   

    if(formdata):
        if((len(formdata) == 3) and ('engineering' in formdata.keys()) and ('science_verification' in formdata.keys()) and ('qa_state' in formdata.keys()) and (formdata['engineering'].value == 'EngExclude') and (formdata['science_verification'].value == 'SvInclude') and (formdata['qa_state'].value == 'NotFail')):
            # This is the default form state, someone just hit submit without doing anything.
            pass
        elif (formdata.keys() == ['orderby']):
            # All we have is an orderby - don't redirect
            pass
        else:
            # Populate selection dictionary with values from form input
            updateselection(formdata, selection)
            # builds URL, clears formdata, refreshes page with updated selection from form
            urlstring = selection_to_URL(selection)
            formdata.clear()
            util.redirect(req, '/searchform' + urlstring + args_string)

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
    req.write('<input type="hidden" id="args" name="args" value="%s">' % args_string)
    req.write('<div class="page">')
    req.write('<form class="searchform" action="/searchform" method="POST">')
   
    form_html_updt = updateform(form_html, selection)
    req.write(form_html_updt)
    selectionstring = selection_to_URL(selection)

    if(selection):
        req.write('<input type="hidden" id="url" value="%s%s">' % (selectionstring, args_string))
   
    req.write('</form>')
    req.write('<hr noshade>')
    # Uncomment this for form processing selection debugging...
    # req.write('<p>selection: %s</p>' % selection)
    req.write('<div id="searchresults" class="searchresults">')
    req.write('<span id="notloading"><P>Set at least one search criteria above to search for data. Mouse over the (text in brackets) to see more help for each item.</P></span>')
    req.write('<span id="loading" style="display:none"><p><img src="/htmldocs/ajax-loading.gif">  Loading...</p></span>')
    req.write('</div>')
    req.write('</div>')
    req.write('</body></html>')

    return apache.OK

def updateform(html, selection):
    """
    Receives html page as a string and updates it according to values in the selection dictionary.
    Pre-populates input fields with said selection values
    """
    for key in selection.keys():
        if key in ['program_id', 'observation_id', 'data_label']:
            # Program id etc 
            html = html.replace('name="program_id"', 'name="program_id" value="%s"' % selection[key])

        elif key in ['date', 'daterange']:
            # If there is a date and a daterange, only use the date part
            if('date' in selection.keys() and 'daterange' in selection.keys()):
                key = 'date'
            html = html.replace('name="date"', 'name="date" value="%s"' % selection[key])

        elif key in ['ra', 'dec', 'sr', 'object', 'cenwlen']:
            # These are all the text fields that don't need anything special
            html = html.replace('name="%s"' % key, 'name=%s value="%s"' % (key, selection[key]))

        elif key in ['inst', 'observation_class', 'observation_type', 'filter', 'resolver', 'binning', 'disperser', 'mask', 'gmos_grating', 'detector_roi', 'qa_state']:
            html = html.replace('value="%s"' % selection[key], 'value="%s" selected' % selection[key])

        elif key in ['spectroscopy', 'mode']:
            if (selection[key] is False):
                html = html.replace('value="imaging"', 'value="imaging" selected')
            else:
                if ('mode' in selection.keys() and selection['mode'] == 'MOS'):
                    html = html.replace('value="MOS"', 'value="MOS" selected')
                elif ('mode' in selection.keys() and selection['mode'] == 'IFU'):
                    html = html.replace('value="IFU"', 'value="IFU" selected')
                elif ('mode' in selection.keys() and selection['mode'] == 'LS'):
                    html = html.replace('value="LS"', 'value="LS" selected')
                else:
                    html = html.replace('value="spectroscopy"', 'value="spectroscopy" selected')

        elif key == 'engineering':
            if (selection[key] is True):
                html = html.replace('value="EngOnly"', 'value="EngOnly" selected')
            elif (selection[key] is False):
                html = html.replace('value="EngExclude"', 'value="EngExclude" selected')
            else:
                html = html.replace('value="EngInclude"', 'value="EngInclude" selected')

        elif key == 'science_verification':
            if (selection[key] is True):
                html = html.replace('value="SvOnly"', 'value="SvOnly" selected')
            elif (selection[key] is False):
                html = html.replace('value="SvExclude"', 'value="SvExclude" selected')
            else:
                html = html.replace('value="SvInclude"', 'value="SvInclude" selected')
        elif key == 'gmos_focal_plane_mask':
            if (selection[key] in ['NS2.0arcsec', 'IFU-R', 'focus_array_new', 'Imaging', '2.0arcsec', 'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec', 'IFU-2', 'NS1.5arcsec', '0.75arcsec', '1.0arcsec', '0.5arcsec']):
                html = html.replace('value="%s"' % selection[key], 'value="%s" selected' % selection[key])
            else:
                # Custom mask name
                html = html.replace('value="custom"', 'value="custom" selected')
                html = html.replace('id="custom_mask"', 'id="custom_mask" value=%s' % selection[key])
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
        # if we got a list, there are multiple fields with that name. This is true for filter at least
        # Pick the last one
        if(type(formdata[key]) is list):
            value = formdata[key][-1].value
        else:
            value = formdata[key].value
        if key == 'program_id':
            # if the string starts with progid= then trim that off
            if value[:7] == 'progid=':
                value = value[7:]

            # Ensure it's upper case
            value = value.upper()

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
        elif key in ['ra', 'dec', 'sr', 'cenwlen']:
            # Check the formatting of RA, Dec, search radius values. Keep them in same format as given though.
            
            # Eliminate any whitespace
            value = value.replace(' ', '')

            # Should do more format verification here?
            # but don't try and interpret it here.

            # Put into selection dictionary
            selection[key] = value

        elif key == 'engineering':
            if value == 'EngExclude':
                selection[key] = False
            elif value == 'EngOnly':
                selection[key] = True
            if value == 'EngInclude':
                # dummy value
                selection[key] = 'Include'
        elif key == 'science_verification':
            if value == 'SvExclude':
                selection[key] = False
            elif value == 'SvOnly':
                selection[key] = True
            if value == 'SvInclude':
                if key in selection.keys():
                    selection.pop(key)

        elif key == 'gmos_focal_plane_mask':
            if value == 'custom':
                selection[key] = formdata['custom_mask'].value
            else:
                selection[key] = value
        elif key == 'custom_mask':
            # Ignmore - done in gmos_focal_plane_mask
            pass
        else:
            selection[key] = value


def nameresolver(req, things):
    """
    A name resolver proxy. Pass it the resolver and object name
    """

    if(len(things) != 2):
        return apache.HTTP_NOT_ACCEPTABLE

    resolver = things[0]
    target = things[1]

    urls = {
        'simbad': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/S?',
        'ned': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/N?',
        'vizier': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/V?'
    }

    if resolver not in urls.keys():
        return apache.HTTP_NOT_ACCEPTABLE


    url = urls[resolver] + target

    urlfd = urllib.urlopen(url)
    xml = urlfd.read()
    urlfd.close()

    req.write(xml)
    return apache.OK
