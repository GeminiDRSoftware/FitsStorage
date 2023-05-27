## Header update interface

### Introduction

The Fits Servers provide a web interface that allows clients to request header
updates for files stored in the system. This allows SOSs et al to modify
things like RELEASE date, raw site quality values, and QA states, without 
having to manipulate the FITS files directly. This is also used by the ODB
for setting such values.

An authentication cookie needs to be sent with the request to provide access
control.

This is preferable to clients directly modifying files on /dataflow for many
reasons. Not least of these is the avoidance of typos etc in the keywords and 
values, but also it support the "FitsStorage looks after the FITS files"
philosophy - the client needs no knowledge of where the file resides in the 
file system for example, and also we can take care of adding the modified
file to the ingest queue without waiting for a filesystem rescan or worrying 
about whether the file is too old to get re-scanned automatically

### How it works.

The client connects to the /update_headers URI, providing the authentication
cookie in the headers, and POSTs a short JSON document detailing the header
updates requested. 

The server responds with a short JSON document providing a response to each
request. This merely indicates that the request has been accepted, and does
not mean it has been completed. The actual process for completing the change
is somewhat asynchronous, and if there are a lot of updates requested, it could
take some time for them all to complete, so we do not hold the http connection 
open while that happens; we do some sanity checking of the request, and if it
_looks good, we reply that we're accepting the request and close the connection._

Once the server accepts the request, the process is somewhat asynchronous. An
entry is added to the fileops queue. When that entry queue entry gets serviced,
the file is fetched from S3 if necessary, the FITS headers are modified, and
the file is stored back to S3 if necessary. The file is then added to the
ingest queue. Once that entry in the ingest queue is serviced, the file will
show up in the database with the updated headers.

### Details

The HTTP cookie for authorization is called 'gemini_api_authorization'

The json POSTed to the server looks like this:
{'request': [{'filename': fn, 'values': actions, 'reject_new':reject_new}, ...],
'batch': False}

In the old api_backend.log, we see:
INFO: Calling set_image_metadata with arguments {'path': '/sci/dataflow/S20220524S0038.fits', 'changes': {'RAWGEMQA': 'USABLE', 'RAWPIREQ': 'YES'}, 'reject_new': False}
INFO: fits_apply_changes: /sci/dataflow/S20220524S0038.fits [{'RAWGEMQA': 'USABLE', 'RAWPIREQ': 'YES'}]

examples of the JSON sent from fixHead.py:
{'request': [
    {'filename': 'N20221215S0001.fits', 
     'values': {'generic': [('RAWIQ', '70-percentile')]}, 
     'reject_new': True}],
'batch': False}

same but for multiple files:
{'request': [
    {'filename': 'N20221222S0002.fits', 
     'values': {'generic': [('RAWIQ', '70-percentile')]}, 
     'reject_new': True}, 
    {'filename': 'N20221222S0003.fits', 
     'values': {'generic': [('RAWIQ', '70-percentile')]}, 
     'reject_new': True}, 
    {'filename': 'N20221222S0004.fits', 
     'values': {'generic': [('RAWIQ', '70-percentile')]}, 
    'reject_new': True}
    ], 
'batch': False}

qa state:
{'request': [
    {'filename': 'N20221215S0001.fits', 
     'values': {'qa_state': 'usable'}, 
     'reject_new': True}], 
'batch': False}

release date:
{'request': [
    {'filename': 'N20221215S0001.fits', 
     'values': {'release': '2025-01-01'}, 
     'reject_new': True}], 
'batch': False}

There's an "old format" which consisted of the list which is now in 'request'
(ie didn't have the batch flag). The ODB uses this. For example:

[{'data_label': 'GS-2023A-Q-136-121-001',
  'values': {'qa_state': 'Pass'}}]


Another example, showing server side interpretation of QA state
{'request': [
    {'filename': 'S20230520S0019.fits', 
     'values': {'raw_site': 'iqany'}, 
     'reject_new': True}], 
 'batch': False}

So the bottom line is the JSON can either be:

THING

or:

{'request': THING, 'batch': Bool}

Where THING is a list of dicts, where each dict looks like:
{'filename' or 'data_label': string value,
 'values': dict,
 'reject_new': boolean value}

where reject_new = True means that we should refuse to insert new header
keywords and only modify existing ones. This is intended as a protection against
typos in the keyword names

and the values dict is a dictionary containing one or more of the following:
'qa_state': 'Pass' / 'Fail' / 'Usable' / etc
'raw_site': 'iqany' / etc
'release': 'yyyy-mm-dd'
'generic': list of tuples [('KEYWORD', 'VALUE')]

question - how do you specify multiple raw_site updates in the same request?