"""
This is the searchform module
"""

# This will only work with apache
from mod_python import apache, util

from .selection import getselection, selection_to_URL
from .summary import summary_body
from .calibrations import calibrations

from ..fits_storage_config import fits_aux_datadir
import os
import urllib

from ..gemini_metadata_utils import GeminiDataLabel, GeminiObservation

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
    #    Re-direct the user to the new URL (without any formdata)
    # Pre-populate the form fields with what is now in our selection dictionary
    #  by modifying the form html server side before we send it out
    # Send out the form html
    # Send out the results html in-line with that, no ajax or anything
    # User messes with input fields
    # User hits submit - back to top

    # grab the string version of things before getselection() as that modifies the list.
    thing_string = '/' + '/'.join(things)
    selection = getselection(things)
    formdata = util.FieldStorage(req)


    # Also args to pass on to results page
    args_string = ""
    if orderby:
        args_string = '?orderby=%s' % orderby[0]

    if formdata:
        if ((len(formdata) == 4) and
                ('engineering' in formdata.keys()) and (formdata['engineering'].value == 'EngExclude') and
                ('science_verification' in formdata.keys()) and (formdata['science_verification'].value == 'SvInclude') and
                ('qa_state' in formdata.keys()) and (formdata['qa_state'].value == 'NotFail') and
                ('Search' in formdata.keys()) and (formdata['Search'].value == 'Search')):
            # This is the default form state, someone just hit submit without doing anything.
            pass
        elif formdata.keys() == ['orderby']:
            # All we have is an orderby - don't redirect
            pass
        else:
            # Populate selection dictionary with values from form input
            updateselection(formdata, selection)
            # build URL
            urlstring = selection_to_URL(selection)
            if 'ObsLogsOnly' in formdata.keys():
                # ObsLogs Only search
                util.redirect(req, '/obslogs' + urlstring)
                # util.redirect raises apache.SERVER_RETURN, so we're out of this code path now
            else:
                # Regular data search
                # clears formdata, refreshes page with updated selection from form
                formdata.clear()
                util.redirect(req, '/searchform' + urlstring + args_string)
                # util.redirect raises apache.SERVER_RETURN, so we're out of this code path now

    req.content_type = "text/html"
    req.write('<!DOCTYPE html><html><head>')
    req.write('<meta charset="UTF-8">')
    req.write('<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>')
    req.write('<script src="/titlebar.js"></script>')
    req.write('<script src="/form.js"></script>')
    req.write('<link rel="stylesheet" type="text/css" href="/whoami.css">')
    req.write('<link rel="stylesheet" type="text/css" href="/titlebar.css">')
    req.write('<link rel="stylesheet" type="text/css" href="/form.css">')
    req.write('<link rel="stylesheet" type="text/css" href="/table.css">')
    req.write('<title>Gemini Archive Search Form</title></head><body>')

    req.write(titlebar_html)

    req.write('<input type="hidden" id="things" name="things" value="%s">' % thing_string)
    req.write('<input type="hidden" id="args" name="args" value="%s">' % args_string)
    req.write('<div class="page">')
    req.write('<form class="searchform" action="/searchform" method="POST">')

    form_html_updt = updateform(form_html, selection)
    req.write(form_html_updt)

    req.write('</form>')
    # Uncomment this for form processing selection debugging...
    # req.write('<p>selection: %s</p>' % selection)

    # playing around with the idea of 'tabs'
    if selection:
        req.write('<ul class="tabs"><li><a href="#" id="resultstab" class="current">Search Results</a></li><li><a href="#" id="caltab">Load Associated Calibrations</a></li><li><a href="#" id="obslogstab">Load Associated Observation Logs</a></li></ul>')
        req.write('<div class="frames">')
        req.write('<div id="searchresults" class="searchresults">')
        req.write('<span id="loading"><img src="/ajax-loading.gif" alt="">  Loading...<br /></span>')
        summary_body(req, 'searchresults', selection, orderby)
        req.write('</div><div id="calibration_results" class="searchresults">')
        req.write('<span id="loading_cals"><br /><img src="/ajax-loading.gif" alt="">  Finding Associated Calibrations... This can take several seconds depending on the size of your calibration association search...</span>')
        req.write('<span id="not_loading_cals"><br />You cannot do calibration association on an unconstrained search, or one that hits the search limit. Please revise your original search so that this is not the case.</span>')
        req.write('</div><div id="obslog_results" class="searchresults">')
        req.write('</div></div>')
    #req.write('<div id="searchresults" class="searchresults">')
    #if selection:
    #    req.write('<span id="loading"><p><img src="/ajax-loading.gif">  Loading...</p></span>')
    #    summary_body(req, 'searchresults', selection, orderby)
    else:
        req.write('<P>Set at least one search criteria above to search for data. Mouse over the (text in brackets) to see more help for each item.</P>')
    req.write('</div>')
    req.write('</body></html>')

    return apache.HTTP_OK

def updateform(html, selection):
    """
    Receives html page as a string and updates it according to values in the selection dictionary.
    Pre-populates input fields with said selection values
    """
    for key in selection.keys():
        if key in ['program_id', 'observation_id', 'data_label']:
            # Program id etc
            # don't do program_id if we have already done obs_id, etc
            if key == 'program_id' and ('observation_id' in selection.keys() or 'data_label' in selection.keys()):
                pass
            else:
                html = html.replace('name="program_id"', 'name="program_id" value="%s"' % selection[key])

        elif key in ['date', 'daterange']:
            # If there is a date and a daterange, only use the date part
            if 'date' in selection.keys() and 'daterange' in selection.keys():
                key = 'date'
            html = html.replace('name="date"', 'name="date" value="%s"' % selection[key])

        elif key in ['ra', 'dec', 'sr', 'object', 'cenwlen']:
            # These are all the text fields that don't need anything special
            html = html.replace('name="%s"' % key, 'name=%s value="%s"' % (key, selection[key]))

        elif key == 'mode':
            if  selection[key] == 'MOS':
                html = html.replace('value="MOS"', 'value="MOS" selected')
            elif selection[key] == 'IFS':
                html = html.replace('value="IFS"', 'value="IFS" selected')
            elif selection[key] == 'LS':
                html = html.replace('value="LS"', 'value="LS" selected')

        elif key == 'spectroscopy' and 'mode' not in selection.keys():
            if selection[key] is True:
                html = html.replace('value="spectroscopy"', 'value="spectroscopy" selected')
            else:
                html = html.replace('value="imaging"', 'value="imaging" selected')

        elif key == 'engineering':
            if selection[key] is True:
                html = html.replace('value="EngOnly"', 'value="EngOnly" selected')
            elif selection[key] is False:
                html = html.replace('value="EngExclude"', 'value="EngExclude" selected')
            else:
                html = html.replace('value="EngInclude"', 'value="EngInclude" selected')

        elif key == 'science_verification':
            if selection[key] is True:
                html = html.replace('value="SvOnly"', 'value="SvOnly" selected')
            elif selection[key] is False:
                html = html.replace('value="SvExclude"', 'value="SvExclude" selected')
            else:
                html = html.replace('value="SvInclude"', 'value="SvInclude" selected')

        elif key == 'focal_plane_mask':

            if 'inst' in selection.keys() and selection['inst'].startswith('GMOS'):
                if (selection[key] in ['NS2.0arcsec', 'IFU-R', 'focus_array_new', 'Imaging', '2.0arcsec', 'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec', 'IFU-2', 'NS1.5arcsec', '0.75arcsec', '1.0arcsec', '0.5arcsec']):
                    html = html.replace('value="%s"' % selection[key], 'value="%s" selected' % selection[key])
                else:
                    # Custom mask name
                    html = html.replace('class="mask" value="custom"', 'class="mask" value="custom" selected')
                    html = html.replace('id="custom_mask"', 'id="custom_mask" value=%s' % selection[key])
            else:
                html = html.replace('class="mask" value="%s"' % selection[key], 'class="mask" value="%s" selected' % selection[key])

        elif key == 'detector_config':
            for item in selection[key]:
                html = html.replace('value="%s"' % item, 'value="%s" selected' % item)

        elif key == 'filter':
            # Generic filter pulldown
            # Only mark filter for the selected instrument. Cannot specify filter with inst = Any anyway
            if 'inst' in selection.keys():
                inst = selection['inst']
                if inst.startswith('GMOS'):
                    inst = 'GMOS'
                html = html.replace('class="%sfilter" value="%s"' % (inst, selection[key]), 'class="%sfilter" value="%s" selected' % (inst, selection[key]))

        elif key == 'exposure_time':
            # Only update the one for the instrument selected
            if 'inst' in selection.keys():
                inst = selection['inst']
                if inst.startswith('GMOS'):
                    inst = 'GMOS'
                html = html.replace('class="%sexpT"' % inst, 'class="%sexpT" value="%s"' % (inst, selection[key]))

        elif key == 'disperser':
            # GPI has custom values
            if 'inst' in selection.keys():
                inst = selection['inst']
                if inst == 'GPI':
                    html = html.replace('class="GPIdisperser" value="%s"' % selection[key], 'class="GPIdisperser" value="%s" selected' % selection[key])
            else:
                # Generic disperser pulldown
                html = html.replace('class="disperser" value="%s"' % selection[key], 'class="disperser" value="%s" selected' % selection[key])
        else:
            # This does all the generic pulldown menus
            html = html.replace('value="%s"' % selection[key], 'value="%s" selected' % selection[key])

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
        if type(formdata[key]) is list:
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

            if go.observation_id:
                selection['observation_id'] = value
            elif dl.datalabel:
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

        elif key == 'focal_plane_mask':
            if value == 'custom':
                if 'custom_mask' in formdata.keys():
                    selection[key] = formdata['custom_mask'].value
            else:
                selection[key] = value
        elif key == 'custom_mask':
            # Ignore - done in focal_plane_mask
            pass
        elif key in ['gmos_speed', 'gmos_gain', 'nod_and_shuffle', 'niri_readmode', 'well_depth', 'nifs_readmode']:
            if 'detector_config' not in selection.keys():
                selection['detector_config'] = []
            selection['detector_config'].append(value)
        else:
            # This covers the generic case where the formdata key is also
            # the selection key, and the form value is the selection value
            selection[key] = value


def nameresolver(req, things):
    """
    A name resolver proxy. Pass it the resolver and object name
    """

    if len(things) != 2:
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
    return apache.HTTP_OK
