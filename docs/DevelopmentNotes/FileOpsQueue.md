## Data Operations Queue

### WTF
The FITS data files are owned by the fitsdata unix user id, but any code
invoked as a result of a hit on the web interface runs as a different user id,
as good security practice. In the old days under mod_apache this was the apache
user id, under wsgi this is fitsweb. (Actually for quite some time, it was 
just running as fitsdata, but we'll not dwell on that).

As a result, when the web interface needs to invoke something that modifies a
fits file, it needs to call another process running as fitsdata to do so. Use
cases for this are the update_header and upload_file URIs for example.

In the old days, we had a setuid script that the web code would call via 
subprocess, but that's obviously not elegant. This was replaced by a so-called
"API" which was basically a python simple web server listening on 
localhost:8000 and running as fitsdata. This was the 'api_backend.py' script,
which evolved to the point where it was using wsgiref.simple_server to
implement a web server that called WSGI code which in turn implemented a 
home-grown protocol which could be described as somewhat similar to json-rpc.

With the added support for header_update calls, it's become very convoluted,
and also there's no access control other than it being on localhost:8000
and as it's implemented with wsgiref.simple_server it can only process one
request at a time and there's no real queueing.

While this has been referred to as an "API", it really isn't an application
programmer interface in any meaningful way. It's simply an interface between
two parts of the code running under different (ideally) user ids.

### What's actually needed here

Basically we need a way to send requests from a client process running as 
fitsweb to a server process running as fitsdata on the same machine. 
This does not a-priori need to be network based (ie no requirement for the 
client and server to be different machines). Ideally we should be able to
process more than one request simultaneously, or at least have a means to
queue up requests in the order received or by some defined priority.
We do need to be able to send a reply back from the server to the client.

### Options:
1) some kind of http based web service. This is what we have now, albeit 
in a complicated manner, and it seems to be somewhat overkill.
2) Simple socket based interface. Feels like reinventing the wheel, but could
be a good way to go. If the socket is a fifo, that solves the access control
issues, but then it's more complex to handle multiple requests at once.
3) xmlrpc or jsonrpc -  again, seems over-complicated. Would need some 
kind of security layer.
4) FitsStorage queue - this would essentially use the database as the IPC
layer, which solves all the access control issues. The only catch is that the
current queues don't provide a means to send a reply back to the thing that
originally made the queue entry.

### Exploring using a FitsStorage queue.

This seems an attractive option. How would this work? 

As with the other queues, there would be a service_fileops_queue.py script 
that would poll the queue and process queue entries in a determined priority 
order - this could be simply first in first out, or we could prioritize in 
some other way. This process would run as fitsdata (as per the other queue
service scripts) and would do that actual manipulations of the data files.

"Clients" - ie the web code running as fitsweb - wanting to invoke file
operations would add an entry to the fileops queue, and wait for it to be
serviced.

The main difference here with the other FitsStorage queues is that normally
there is no feedback from the service_queue script to the requester. There
are several ways we could implement this though:
1) The service_queue code could write a response into the queue entry row
when it completes instead of deleting the queue entry. The "client" side would
then read the response back and delete the queue entry itself. This could
require polling, or it may be possible to use the postgres listen/notify
system.
2) We could use the postgres listen/notify system to send the message back to
the client, but there's no SQLAlchemy support for this, so it could get messy,
and also it's not SQL standard so would only work with postgres - that's not
really a problem though.
3) The service_queue script could write the result to a pre-determined 
location on the file-system (eg define a directory and then use the fileops
queue id as the filename, from which the caller would read it. This could get
messy with files left behind, but that should be simple to curate, and also
the client could use inotify or similar for efficiency.

Initial thoughts are that 1 or 3 are the best options. suggest we try 1, and it 
should be simple to refactor to switch if needed.

## Code structure

* service_fileops_queue.py:
  * instantiate a FileOpser class, storing session and logger in it.
  * pops the queue (sets inprogress=True)
  * passes the fileopsqueueentry to fileopser.do_fileop()

* FileOpser class (analogous to Exporter and Importer) - stores the session
and logger instances.
  * do_fileop():
    * Decodes the request 
    * calls the python function to actually do the work
    * Puts the return values in the response 
    * commits the database.
