Visiting Instruments
====================

General Handling
----------------

While our primary data comes in via the `fits-copy_from_dhs` service, the rest is either
placed directly onto dataflow, or it is copied from various locations by the `fits-copy_from_visiting_instrument`
service.  This service handles source data from multiple locations, copies into datestamped folders
for easy management, and performs any needed header fixes to the data during the copy.  The service
uses command line arguments to tell the scripts which visiting instruments to support on that host.  This
is done by splitting the service definition in the `otherfiles` folder into two -
`etc_systemd_system_fits-copy_from_visiting_instrument_mko` for Hawaii and
`etc_systemd_system_fits-copy_from_visiting_instrument_cpo` for Chile.  The visiting instruments
currently supported in the scripts are 'Alopeke, Zorro, and IGRINS.

The script that is run to do the copy is `fits_storage/scripts/copy_from_visiting_instrument.py`.  This
script makes use of `header_fixer2.py` in the same folder to make various header keyword repairs.  The
typical flow is:

 * Find all datafiles for the instrument in it's folder, in date format subfolders
 * For each datafile, if it is already known to the system we skip it
 * For new datafiles, check and fix the header keywords using the instrument-specific logic in `header_fixer2.py`
 * the `header_fixer2.py` will add a `COMMENT` to the header if it had to repair anything
 * copy the repaired files into `instrument/date/` folders under `/sci/dataflow`
 * add the copied files to the ingest queue to be handled normally from there on out

'Alopeke & Zorro
----------------

Header Repair
"""""""""""""

These two instruments are mostly the same.  'Alopeke operates in Hawaii and Zorro is in Chile.  Some of the problems
we see in the original headers are:

 * `OBSID` and `DATALAB` matching the `GEMPRGID` instead of adding to it
 * `Object` keyword instead of `OBJECT`
 * `OBSTYPE` keyword with lowercase value
 * `CTYPEx` of `RA--TAN` instead of `RA---TAN`
 * `CRVALx` of string type instead of float
 * missing `OBSTYPE`, `OBSCLASS`, `TELESCOP`, `INSTRUME`, `DATE-OBS`, and/or `RELEASE`

Staging Area
""""""""""""

'Alopeke data lives in `/net/mkovisdata/home/alopeke/`.  Zorro data lives in Chile on
`/net/cpostonfs-nv1/tier2/ins/sto/zorro/`


IGRINS
------

Header Repair
"""""""""""""

IGRINS has a different set of header issues that we fix.  These have been as follows

 * `GEMPRID` instead of `GEMPRGID` keyword
 * `OBSID` and `DATALAB` missing or equal to `GEMPRGID`
 * missing `DATE-OBS` and/or `RELEASE`

Staging Area
""""""""""""

IGRINS data lives in `/net/cpostonfs-nv1/tier2/ins/sto/igrins/DATA`
