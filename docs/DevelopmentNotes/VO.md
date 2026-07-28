## Notes on implementing VO interfaces

It would be good to support VO interfaces to the archive. Notably all the data
(both raw and reduced, though especially reduced) should be available via
Simple Image Access (SIA). SIA is not a download server, it's more of a data
discovery (ie search) mechanism. The results contain the URL from which the
file can be downloaded, and this would be a standard FitsStorage /file URL.

It would also be good to support various forms of TAP queries - we *could*
expose direct queries to the FitsStorage tables (such as header and diskfile 
through this, though it's not obvious to me what the end-user use cases are for
that. However, we when we have things like source detection tables from reduced
imaging, there are likely end-user use cases for access to these, and there may
be use cases for TAP access to instrument and/or site-monitoring data.

These both rely on the presence of a table called ivoa.obscore, which basically
corresponds to an IVOA standard header table. It may be possible to implement
this as a view of the header table, or it may be preferable to maintain a
separate table. We could at least initially only populate this with public 
datasets, to avoid needing to implement access control on the VO interfaces.

This will require population (or calculation) of the processing level (PROCLVL)
keyword as obscore has an equivalent. It would probably be simplest to adopt
the VO values for PROCLVL rather than accounting for the off-by-one (VO 
considers level 0 to be instrument specific data format and level 1 to be
RAW data but in FITS, so in practive we don't have any level 0 data)

Both SIA and TAP use ADQL, which is an extension of a subset of SQL. The
simplest approach seems to be to use https://github.com/aipescience/queryparser
as a standalone module (ie not as part of daiquiri) to translate the ADQL into
SQL to execute directly. This would require us to adopt pgSphere (which would
probably be a good thing anyway).

We would need to implement (or adopt code to implement) the broader
SIA / TAP server. It may be viable to implement this within the existing 
FitsStorage web architecture, though there are probably lots of annoying details
to contend with. Or we could run a completely separate web service to host this,
with suitable restricted database access.

Alternatively, once we have obscore, we may be able to adopt a something like
the cadc tap server, which would handle both the ADQL and server code, though
it's big and complicated. We'd certainly want to run this separately from the
main archive I think.

Path:

* Adopt pgSphere
* Populate or calculate-on-the-fly processing level values
* Create ivoa.obscore table / view

Then, either:

* queryparser
* implement services

or:

* opencadc tap service