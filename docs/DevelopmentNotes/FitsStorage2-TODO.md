## TODO list for FitsStorage2 being ready for operational use

Note, nice-to-haves and enhancements should be filed as issues on github
and tagged as enhancement. This list is for stuff that *needs* to be done
in order to get the new code base into production use. It's divided into two
lists, one for the summit fits servers, the other for the archive. Oh, and
another one for the tape server. And another one to keep track of future things
so that they don't keep getting folded back into the archive list

### Summit Fits Servers

* Visitor instrument data header fixing and ingest
* FITS header update interface that the SOSs use (was api, now fileops)
* Re-implement a queue status display
* Test config files with sections per host and make central config file repo
* QAmetric DB
* Notifications
* /calibrations URL
* gmos calibrations urls


### Archive

* Fix previews (images at least)
* "/ingest_programs"
* publications
* miscfiles



### Tape Server

* Test and convert the whole tape system I guess.


### Future

* Provenance and History
* Fix spectroscopy Previews
* Authenticate gemini users against central sign on
* header updates as file upload hints.