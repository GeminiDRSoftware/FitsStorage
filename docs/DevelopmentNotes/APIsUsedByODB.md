# FITS Storage APIs (used by the ODB and others)

## jsonfilelist
For example:
* jsonfilelist/present/filename=N20001122S1234.fits
(at least, I think this is the ODB, it's on archive, so it's difficult to
tell where inside gemini it comes from)

## jsonqastate
For example :
* /jsonqastate/Gemini-North/present/RAW/obsid=GN-2020B-LP-106-13
* /jsonqastate/Gemini-North/present/RAW/N20240820
* /jsonqastate/Gemini-South/present/RAW/S20240821
* /jsonqastate/Gemini-South/present/RAW/20240815-20240821

## update_headers
* POST to /update_headers
* Must supply 'gemini_fits_api_authorization' cookie
* POST data is a JSON document describing the header update requested.
* Fits storage code currently supports two formats for this, the ODB uses the 
"old" one. I think the only thing using the new one is fixHead.py. We should 
convert that and remove the "new" format, it doesn't add anything.

Note that you can identify the file to update by data label or filename.
Filename should be preferred, as data label is going to become ambiguous as
reduced data proliferates in the archive and may have the same data label as
the raw data. ODB currently uses datalabel. Consider making update_headers
apply to RAW data by default.

Note that you can request multiple updates to the same file in a single request
by including multiple keys in the `values` dictionary. This will be more 
efficient on the server side than sending multiple requests. 

Note that you can send requests to update multiple files in the same http 
POST. The "Old format" is a list - just send a list of requests. In the "New 
Format", the "request" entry is a list, one entry per file to update. In the
current implementation, this looks broken. The ODB doesn't use it, but the
fixHead.py SOS script may do.


### "Old Format" examples:

```
[{"data_label": "GN-2018A-FT-103-13-003", 
  "values": {"raw_site": "iqany"}}]
```

```
[{"filename": "N20180329S0134.fits", 
  "values": {"qa_state": "Usable"}}]
```

### "New Format" examples:

```
{"request": [{"data_label": "GN-2018A-FT-103-13-003", 
              "values": {"release": "2123-04-05"}, 
              "reject_new": true}], 
"batch": false}
```

```
{"request": [{"filename": "N20180329S0134.fits", 
              "values": {"raw_site": "iq70"}, 
              "reject_new": true}], 
"batch": false}
```

### Return Value

The update_headers endpoint returns a JSON document describing whether the
request was accepted or not. Note that the actual header update is performed
asynchronously, and the endpoint cannot return a message indicating the success
or failure of the actual header update. All it can do is say if we accepted the
request. Under normal load conditions the update should be fully processed
within a few seconds, but this will vary significantly - the requests
are handled in a queue which generally prioritizes handing of more recent data
files, so updates to older files may see longer wait times if there are also
requests to handle involving newer files.

The content type of the html response will be 'application/json'

Successful result:
```
[{"result": true, "id": "N20180329S0134.fits"}]
```
(id may be missing if unknown)

Error condition:
```
[{'result': false, 'error': 'No filename or datalabel given'}]
```

The result is a list, and *should* give a status for each file in the request.
However, this looks like it's currently broken, in the old code if it
encounters an error, it bails out and sends an error result, and you don't
really know which item in the request generated an error.