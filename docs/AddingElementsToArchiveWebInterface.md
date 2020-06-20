# How to Add an new element to the Fitsstore web interface.

Our example will be the real world addition of the All-Sky Camera to the database and web interface.

If this is a new data type for Astrodata, you will need to add an astrodata adclass for that data type.
In our example, these data are for instrument, “ALLSKYCAMERA”.

# Update for Database

Update AstroData to accommodate the new data type in the database.
How to write your adclass is beyond the scope of this document. Consult the AstroData manual on
how to do this.

# Fitsstore updates

## `fits_storage/orm/header.py`

In `orm/header.py`, add the new column (field) name to the header table.
These are set as class attributes on the `header.Header` class.

```
    # added per Trac #264, Support for Gemini South All Sky Camera

    site_monitoring = Column(Boolean)
```

Add some conditions to header.populate_fits() if needed.

	E.g., somewhere in that giant function, check the tags and do something like

```
	if “GS_ALLSKYCAM” in ad.tags:
	    self.site_monitoring = True
```

    or, as recommended by RC, put this into a function in gmu module (gemini_metadata_utils.py).
    Indeed, this was done, and the function defined thusly:

```
	def site_monitor(string):
    	    """
    			Parameters:
    			----------
    			string: <str>
    			    The name of the instrument that is a sky monitor. Currently, this
            supports only GS_ALLSKYCAMERA. The string will generally be that
            returned by the astrodata descriptor, ad.instrument().

    			Return
    			------
    			yes: <bool>
        			Returns True when GS_ALLSKYCAMERA is passed.

    			"""
    			if string == 'GS_ALLSKYCAMERA':
        	    return True
    			else:
        		    return False
```

## `fits_storage/web/searchform.py`

In `web/searchform.searchform()` add the new field to the formdata conditional tuple:

```
	def searchform(things, orderby):
	…
    		if formdata:
       	  if ((len(formdata) == 6) and
          	 ('engineering' in formdata.keys()) and (formdata['engineering'].value == 'EngExclude') and
              ('science_verification' in formdata.keys()) and (formdata['science_verification'].value == 'SvInclude') and
              ('qa_state' in formdata.keys()) and (formdata['qa_state'].value == 'NotFail') and
              ('col_selection' in formdata.keys()) and
              ('site_monitoring' in formdata.keys()) and (formdata['site_monitoring'].value == 'SmExclude') and
              ('Search' in formdata.keys()) and (formdata['Search'].value == 'Search')):

              # This is the default form state, someone just hit submit without doing anything.
              pass
```

Be sure to up the value of the length test for a new item in this set. I.e., if you add another item, the test 

```
    if ((len(formdata) == 6)
```

has to be changed to 

```
    if ((len(formdata) == 7)
```

Add the new key to the if-elif tree in the function, updateform(selection)

```
    def updateform(selection):
    …
    dct = {}
    for key, value in selection.items():
    …
    elif key == 'site_monitoring':
        if value is True:
            dct[key] = 'SmInclude'
        else:
            dct[key] = 'SmExclude'
```

In the same file, `searchform.py`, add this same thing (!) to another function,

```
  def updateselection(formdata, selection):
	  …
      for key in formdata:
          elif key == 'site_monitoring':
              if value == 'SmInclude':
                  selection[key] = True
              elif value == 'SmExclude':
                  selection[key] = False
```

On or about L361, there is a big dictionary for “dropdown options”. Add new dropdown items to this.
sm_options will be the options for site_monitoring. You can add any number of options and option
values to this.

```
dropdown_options = {
    "engdata_options":
        [("EngExclude", "Exclude"),
         ("EngInclude", "Include"),
         ("EngOnly", "Find Only")],
    "mode_options":
        [("imaging", "Imaging"),
         ("spectroscopy", "Spectroscopy"),
         ("LS", "Long-slit spectroscopy"),
         ("MOS", "Multi-object spectroscopy"),
         ("IFS", "Integral field spectroscopy")],
    "svdata_options":
        [("SvInclude", "Include"),
         ("SvExclude", "Exclude"),
         ("SvOnly", "Find Only")],
    "sm_options":
        [("SmExclude", "Exclude"),
         ("SmInclude", "Find Only")],
```

## `fits_storage/web/selection.py`

Add values and defaults to the appropriate “getselection” type. For site monitoring, this is a boolean.

```
getselection_booleans = {
    'imaging': ('spectroscopy', False),
    'spectroscopy': ('spectroscopy', True),
    'present': ('present', True), 'Present': ('present', True),
    'notpresent': ('present', False), 'NotPresent': ('present', False),
    'canonical': ('canonical', True), 'Canonical': ('canonical', True),
    'notcanonical': ('canonical', False), 'NotCanonical': ('canonical', False),
    'engineering': ('engineering', True),
    'notengineering': ('engineering', False),
    'science_verification': ('science_verification', True),
    'notscience_verification': ('science_verification', False),
    'site_monitoring': ('site_monitoring', True),
    'not_site_monitoring': ('site_monitoring', False),
    'photstandard': ('photstandard', True),
    'mdgood': ('mdready', True),
    'mdbad': ('mdready', False),
```

In the function, sayselection,  add a conditional that provides a string for html pasting.

```
def sayselection(selection):
    """
    Returns a string that describes the selection dictionary passed in suitable
    for pasting into html.

    """
    # Collect simple associations of the 'key: value' type from the
    # sayselection_defs dictionary
    parts = ["%s: %s" % (sayselection_defs[key], selection[key])
                for key in sayselection_defs
                if key in selection]
    
    if selection.get('site_monitoring'):
        parts.append('Is Site Monitoring Data')

    …
_____________________________________
goto queryselection_filters,  (currently, selection.py @L313) a tuple linking string labels to database Header attributes.

queryselection_filters = (
    ('present',        DiskFile.present),
    ('canonical',      DiskFile.canonical),
    ('science_verification', Header.science_verification),
    …
    ('readmode',      Header.detector_readmode_setting),
    ('filter',        Header.filter_name),
    ('spectroscopy',  Header.spectroscopy),
    ('mode',          Header.mode),
    ('coadds',        Header.coadds),
    ('mdready',       DiskFile.mdready),
    ('site_monitoring', Header.site_monitoring)
    )
```

In the function, `selection_to_URL`, update key testing in selection object. 
This converts the selection criteria from the searchform and converts to a URL string.

```
def selection_to_URL(selection, with_columns=False):
    """
    Receives a selection dictionary, parses values and converts to URL string.

    """
    urlstring = ''

    elif key == 'site_monitoring':
        if selection[key] is True:
            urlstring += '/site_monitoring'
        else:
            urlstring += '/not_site_monitoring'
    …
	return ulrstring
```

## `FitsStorage/data/templates/search_and_summary/searchform_detail.html`

In this file, `searchform_detail.html`, a template for the searchform is laid out. Add new elements to this template.
For site_monitoring, this is added @L110 of the template:

```
        <select id="site_monitoring" class="ddw" name="site_monitoring">
{{ generate_option_list(sm_options, updated.site_monitoring) }}
        </select>
        Site Monitoring Data
        <span class="help">(help)
            <span class="helpright">Choose whether to include, or exclude site monitoring data.</span>
        </span><br>
```
