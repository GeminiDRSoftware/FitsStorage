2020-2.18
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- added calprog and notcalprog options to search explicitly for calibration programs or to exclude (URL only)

Calibrations
------------

calibration_niri.py
^^^^^^^^^^^^^^^^^^^

- Made data_section parser handle tuple () style inputs and output the legacy Section() format for NIRI


Other
-----

diskfilereport.py
^^^^^^^^^^^^^^^^^

- Added definitions for GMOS WCS extension to validation
- Added definition for PROVENANCE and PROVENANNCE_HISTORY to validation
- Added AWAV option for CTYPE1 and '' option for CTYPE2

ingest_standards.py
^^^^^^^^^^^^^^^^^^^

- Made idempotent for reruns to update existing records by name (or create new ones as needed)
- update to related geometryhacks logic to properly parse coordinate values

add_to_export_queue.py
^^^^^^^^^^^^^^^^^^^^^^

- Updated header query to use session as a keyword argument, was broken

exportqueue.py
^^^^^^^^^^^^^^

- Check file presence on archive with the non-bz2 filename, to avoid problems with A'lopeke and Zorro
- Added destination to unique constraint so we can have multiple destinations

queue.py
^^^^^^^^

- Updated filename regex to support more visiting instruments

list_headers.py
^^^^^^^^^^^^^^^

- Add a flag to allow an unlimited search for internal tools, also updated the add_to_export_queue.py and write_to_tape.py to use it


2020-2.17
=========

Web Services
------------

/jsonsummary
^^^^^^^^^^^^

- added entrytimedaterange query for a range of entry times, plus sort, plus fixes to lastmoddaterange query for CADC

Other
-----

header.py
^^^^^^^^^

- Made header parsing more tolerant of bad FITS files so they ingest with missing fields

2020-2.16
=========

Updated Scripts
---------------

copy_from_dhs
^^^^^^^^^^^^^

- If DHS file is larger than dataflow, copy it again
- flush list of "known" files every 1000 iterations
- perform a fitsverify check after the TELESCOP and astrodata checks
- perform and cache an md5 checksum on DHS files to compare against for future checks against todays data

2020-2.15
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- added f32 options to disperser search for NIRI

Web Services
------------

/programinfojson

- fixed formatting bug that cuased duplicate "s

Other
-----

previewqueue
^^^^^^^^^^^^

- Removed problem if statement for s3 preview generation


2020-2.14
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- updated color for quick look data rows in the grid

Other
-----

dbmigration
^^^^^^^^^^^

- removed initializing timestamps on older file database records since it takes too long during ansible deploy


2020-2.13
=========

Web Services
------------

/calibrations
^^^^^^^^^^^^^

- Filtered out some arc and dark warnings for GMOS per SOS feedback

Updated Scripts
---------------

delete_files.py
^^^^^^^^^^^^^^^

- Fixed column name error for unused, but available, order by date column

Other
-----

fits_storage_config.py
^^^^^^^^^^^^^^^^^^^^^^

- Added new twilight/bias json web APIs for SOSes to blocklist for public archive


2020-2.12
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- Cleanup comment text in log comment table link so it doesn't break html with chars like "
- Added publication field for searching by bibliography code (back-end already has support)

Web Services
------------

/programinfojson
^^^^^^^^^^^^^^^^

- New endpoint for json encoded program information, for Andy Adamson

Other
-----

ingestqueue.py
^^^^^^^^^^^^^^

- commit should have been flush, so all changes rollback on an error

header.py
^^^^^^^^^

- converted Alopeke/Zorro custom AstroData overide into helper methods (for ra/dec)


2020-2.11
=========

Other
-----

local_cals
^^^^^^^^^^

- Updating version number for the calibrations standalone library to 1.0.0


2020-2.10
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- Altered science quality column to `Qual`

/summary
^^^^^^^^

- Removed defaulting logic for engineering, SOSes reported it was a change of behavior


Web Services
------------

/calmgr
^^^^^^^

- Now accepts a `sq` or `ql` term and infers a smart `procmode` query filter against it


Other
-----

header.py
^^^^^^^^^

- `procsci` column renamed to `procmode`
- `procmode` driven off of `PROCMODE` header keyword or, if not found, `PROCSCI` for legacy support
- `AstroDataAlopekeZorro` added to clean up WCS issues and fallback to something that works during Header record creation [#gl10]


Updated Scripts
---------------

alopeke_zorro_wcs_workaround.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- New cleanup script to quick-fix RA/DEC values where missing for existing Alopeke and Zorro records (for archive after deploying the header.py fix above) [#gl10]


previewqueue
^^^^^^^^^^^^

- Fixed bias/gain adjust for GMOS in preview generation to not clip pixels to black [#gl7]

2020-2.9
========

Other
-----

local_cals
^^^^^^^^^^

- Updating version number for the calibrations standalone library to 1.0.0


2020-2.8
========

Other
-----

local_cals
^^^^^^^^^^

- introduced dependency on fits_storage, had to patch to 2020-2.9

2020-2.7
========

/calibrations
^^^^^^^^^^^^^

- Added workaround for SOS missing cals webpage in calibration_gmos to not apply darks if year is 2020+ (i.e. not Ham)

2020-2.6
========

/gmoscalbiasfiles
^^^^^^^^^^^^^

- Made bias file list bugfixes [#398]


2020-2.5
========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- Added B480 grating to GMOS searches


2020-2.4
========

User-Facing Changes
-------------------

/lsummary
^^^^^^^^^

- Added long form summary back into web service (not sure when it was removed) [#399]

/searchform
^^^^^^^^^^^

- Added extra reduction state options in search form for slitillum, standard, etc. [#396]

Web Services
------------

/gmoscalbiasfiles
^^^^^^^^^^^^^

- Made bias file list per SOS request [#398]


2020-2.3
========

Web Services
------------

/gmoscaltwilgihtfiles
^^^^^^^^^^^^^^^^^^^^^

- Updated filelisting api endpoint to return a json dictionary per SOS team [#397]


2020-2.2
========

Other
-----

archive_install.yml
^^^^^^^^^^^^^^^^^^^

- Apply database migrations as user fitsdata to ensure proper table/sequence ownership
- Set migration version to 18 for new databases (step 19 has been eliminated)


2020-2.1
========

Updated Scripts
---------------

database_backup.py
^^^^^^^^^^^^^^^^^^

- Allow existing folder for database backup location [#395]


2020-2.0
===================

User-Facing Changes
----------------------

- Search by GPI Astrometric Standard [#253]
- Fixed time ranges to operate from 2pm-2pm local time in Chile/Hawaii [#288]
- Ability to search for calibrations associated with selected files [#356]
- Support for slitillum calibration data
- Load Cals for Marked disabled if there are no checkboxes to mark [#386]

Service fixes and enhancements
------------------------------

fits_storage
^^^^^^^^^^^^

- Database backup location now calculated from hostname by default [#342]
- jsoncalmgr and xmlcalmgr endpoints to get a json or xml variant of the data, with calmgr returning xml for back compatibility
- summary service accepts POST method calls where the body is a json dictionary merged over the URL selections

Web Services
------------

/login
^^^^^^

- Now accepts a redirect query argument to invoke an http redirect on successful login

/summary
^^^^^^^^

- Now sorts null ut datetimes to the end of the results, so most recent properly displays
- Removed engineering results by default

/tapestuff
^^^^^^^^^^

- Fixed bug in taperead listing, was always blank

/miscfiles
^^^^^^^^^^

- Fixed bug in session/context handling in miscfiles details
- Fixed validation to check both release and program independently (was passing if release passed)

/gmoscaltwilgihtfiles
^^^^^^^^^^^^^^^^^^^^^

- New webservice endpoint to get json list of cal filenames associated with `gmoscaltwilightdetails` counts [#392]

Updated Scripts
---------------

various
^^^^^^^

- Check for `__name__` == `__main__` to avoid side effects during sphinx documentation [#369]

delete_files.py
^^^^^^^^^^^^^^^

- Configurable minimum age for files to delete, will skip any files with a filename pattern implying it's too new
- Error messages also added to a dedicated error email, which is sent only if errors were seen
- Checks export queue and skips files that are awaiting export
- Option to delete over a specific age, using new column in `DiskFile` populated on ingest by interpreting the filename [#376]

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- IGRINS checks for and adds RELEASE keyword if missing (and DATE-OBS exists)
- Allow for multiple filename regexes for 'Alopeke and Zorro
- Add Telescope/Instrument keyword if missing (it is absent in <= 2018 'Alopeke data)
- Fixing exposure time in 'Alopeke and Zorro files if it is a string, should be numeric
- Send error email (if recipient specified) when there is an error copying over/header fixing a file [#378]

service_ingest_queue.py
^^^^^^^^^^^^^^^^^^^^^^^

- via ingestqueue.py, now when it has an error parsing the headers, it will still try to clean up the cache file so z_staging doesn't fill up
- If we have an error during ingest, be sure to cleanup the `z_staging` cache file if it exists, then continue to raise the error

fits_storage_config.py
^^^^^^^^^^^^^^^^^^^^^^

- made preview queue setting per-host configurable to avoid accidentally turning them on for hosts that do not require them

verify_exported.py
^^^^^^^^^^^^^^^^^^

- helper script to check files were exported to the archive

verify_in_diskfiles.py
^^^^^^^^^^^^^^^^^^^^^^

- helper script for checking files on disk exist in the `diskfiles` table.

YouveGotDataEmail.py
^^^^^^^^^^^^^^^^^^^^

- fuzzy handling for email lists, space and comma delimited both work if all terms have @ [#383]

Other
-----

ansible
^^^^^^^

- Building HTML docs on `hbffits-lv4` host and exposing via apache [#368]
- Added boolean variables in inventory to to turn on/off features in ansible deploys, instead of hardcoded hostname logic [#370]
- Created defaults file for archive host variables so we only need to define overrides in the inventories [#370]

calibration
^^^^^^^^^^^

- Normalized calibration matching for GMOS arcs to use amp read area even for processed arcs [#380]

calcachequeue
^^^^^^^^^^^^^

- Added filename to object to use for logging errors in the queue_error table (as is done for other queues)


2020-1.11

Updated Scripts
---------------

header_fixer2.py
^^^^^^^^^^^^^^^^

- Added a fix for CTYPE2 if it is incorrectly set to RA--TAN to fix it to RA---TAN for 'Alopeke and Zorro

User Scripts
------------

fixHead.py
^^^^^^^^^^

- allow for multiple conditions passed using , to separate (added by @bcooper)

Updated Web Services
--------------------

/gmoscaltwilightdetails
^^^^^^^^^^^^^^^^^^^^^^^

- Added extra filtering to find last _flat.fits file when finding date within 6 months
- Added query for all within pat 6 months when no relevant _flat.fits was found (for the common filter/bin types)
- Fix to query to only count headers with a canonical diskfile entry


2020-1.10
=========

Updated Web Services
--------------------

/gmoscaltwilightdetails
^^^^^^^^^^^^^^^^^^^^^^^

- New web report for SOS staff to give details on twilights through previously processed filter/bin combination


2020-1.9
========

Updated Scripts
---------------

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- If OBSCLASS not set for 'Alopeke or Zorro, set to science

copy_from_dhs.py
^^^^^^^^^^^^^^^^

- Increase min age to 15s due to ongoing dhs issues


2020-1.8
========

Updated Scripts
---------------

copy_from_dhs.py
^^^^^^^^^^^^^^^^

- Performs basic validation before doing the copy
- On 4th failure to validate, emails fitsdata@gemini.edu to alert on the failing file

problem_checker.py
^^^^^^^^^^^^^^^^^^

- New script to look for problems with recent data that hasn't ingested correctly

2020-1.7
========

Updated Scripts
---------------

fits_validator.py
^^^^^^^^^^^^^^^^^

- For regex checks, convert non-str values into strings before testing - and automatically fail on None

2020-1.6
========

Updated Scripts
---------------

header_fixer2.py
^^^^^^^^^^^^^^^^

- IGRINS utility call adds RELEASE based on obs date, if the RELEASE date is missing

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Modified IGRINS handling to match folder structure Hwi is using for the staging area I copy from

2020-1.5
========

Updated Scripts
---------------

exportqueue.py
^^^^^^^^^^^^^^

- During export, increased timeout of file post to 10 minutes as Zorro files take time to upload

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Added compression option which, if set to false, does not re-encrypt data after fixing the headers

fixHead.py
^^^^^^^^^^

- Queries server for list of known files on that date and discards any number range entries that are absent in the filenames

verify_exported.py
^^^^^^^^^^^^^^^^^^

- Queries regex list of files and checks if they are present on the archive.  That is, we aren't missing any exports

2020-1.4
========

Updated Infrastructure
----------------------

playbook.yml
^^^^^^^^^^^^

Added user-space crontab install for fitsdata on ops, if absent.  Also added logic to  update CPO crontab to run with
region specific flag

2020-1.3
========

Updated Scripts
---------------

YouGotDataEmail.py
^^^^^^^^^^^^^^^^^^

- Log warning and don't send email for searches with unrecognized terms

2020-1.2
========

Updated Scripts
---------------

odb_data_to_archive.py
^^^^^^^^^^^^^^^^^^^^^^

- Runs programs in batches of 20

2020-1.1
========

Updated Web Services
--------------------

/ingest_program
^^^^^^^^^^^^^^^

- Now takes optionally an array of program data for batch processing

New Web Services
----------------

/publication/ads/<bibcode>
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Get an ADS record for a specific bibliography code

/list_publications
^^^^^^^^^^^^^^^^^^

- Get a list of all bibliography codes in the system

