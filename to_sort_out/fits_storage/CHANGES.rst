2023-1
======

Other
-----

various
^^^^^^^

- Changed references to FitsStorageDB to GeminiObsDB
- Changed default branch for GeminiObsDB and GeminiCalMgr to main per current github practice
- Pushed GeminiObsDB and GeminiCalMgr to github
- Updated ansible/docker/Jenkinsfile to use the github repos for GeminiObsDB and GeminiCalMgr


2022-1
======

User-Facing Changes
-------------------

searchform
^^^^^^^^^^

- Added previous/next day buttons where applicable when a date search has been done

- Calibrations tab honors user column selections

- binning column enabled by default in results tables

- UTC support for datetimes with Z suffix

- LR-IFU and HR-IFU added for GNIRS

fac-pdu.def
^^^^^^^^^^^

- checking for bad filter values

fits_validator
^^^^^^^^^^^^^^

- support for reporting bad filter values sith custom exception
- improved checking of RADECSYS/RADESYS keywords

gemini-fits-validator
^^^^^^^^^^^^^^^^^^^^^

- logic to check for bad filter values

associated_cals_json
^^^^^^^^^^^^^^^^^^^^

- new webpage added for associated calibrations as json [GL#41]

form.js
^^^^^^^

- fix for preview popup on calibrations tab

preview
^^^^^^^

- preview requests fixed to properly 404 on requests for previews that we don't have [GL#32]
- flipping image previews vertically

reporting
^^^^^^^^^

- fitsverify and metadata reports view handling null values in database
- fullheader request for which we don't have a stored full text header will add a text message to that effect in the output

templating
^^^^^^^^^^

- float formatting helper modified to handle None values


Scripts
-------

check_on_tape
^^^^^^^^^^^^^

- fixed pointing of default tape server

copy_from_visiting_instrument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Added date regex option for only adding files with a certain date prefix
- Added interrupt handling to clean up any in progress file being copied

delete_files
^^^^^^^^^^^^

- skip files if we can't connect to the tape service to check, but don't abort the run
- fixed pointing of default tape server
- support for path or partial path selection

local_delete_files
^^^^^^^^^^^^^^^^^^

- fixed pointing of default tape server
- fixed md5 checking logic for tape server

read_from_tape
^^^^^^^^^^^^^^

- list_tape_sizes option for tape sizes
- split log handling for parallel tape runs
- handle large sizes in logging tape read size

write_to_tape
^^^^^^^^^^^^^

- holding commits after a bulk job is done instead of per file to speed things up

repair_alopeke_zorro_wcs
^^^^^^^^^^^^^^^^^^^^^^^^

- Refactored to handle local-to-local file fixes without S3 or DB updates [GL#34]

problem_checker
^^^^^^^^^^^^^^^

- Only check DHS files ending in .fits, so not .fits.temp or whatever is used for in-progress

ingest_pubs
^^^^^^^^^^^

- use new client-side api cookie setting to choose cookie to send in POST

odb_data_to_archive
^^^^^^^^^^^^^^^^^^^

- use new client-side api cookie setting to choose cookie to send in POST

(various).service
^^^^^^^^^^^^^^^^^

- Set to multi-user runlevel for systemd support, services were not starting on boot even when enabled


Other
-----

selection
^^^^^^^^^

- Added filelist as a search term that is not open, allowing for larger count of results/downloads

archive_install.yml
^^^^^^^^^^^^^^^^^^^

- Fixes for SELinux installs on fresh host for PostgreSQL support.
- using enh/ghost_bundle_cal_updates branch of DRAGONS to get GHOST support early

admin_file_permissions
^^^^^^^^^^^^^^^^^^^^^^

- Admin web form for adding/removing per-file and per-obsid permissions to specific users

exportqueue
^^^^^^^^^^^

- Added distinct sortkey column to allow for smarter ordering vs relying on the filename [GL#28]
- Fixed logging messages to use % string format, the logger does not handle an args list approach [GL#30]
- Fixed test to not check old logic of existing entries
- Fixed improper exception import for requests library

preview_queue
^^^^^^^^^^^^^

- Limiting number of frames rendered for 1-D with configurable default to 9

header_fixer2
^^^^^^^^^^^^^

- IGRINS detects malformed "Gemini South" (or "Gemini North") telescope values and fixes file header to Gemini-X [GL#29]

file_parser
^^^^^^^^^^^

- IGRINS parser detects malformed "Gemini South" (or "Gemini North") telescope values and returns Gemini-X [GL#29]

ingestqueue
^^^^^^^^^^^

- Fixed logging messages to use % string format, the logger does not handle an args list approach [GL#30]

calmgr
^^^^^^

- Migrated to safer eval call with manual handling for Section, NonLinCoeffs and datetime types [GL#31]

tapestuff
^^^^^^^^^

- added extra notations to avoid warnings in latest SQLAlchemy
- support for large values for disk size (BigInteger)
- support for JSON list of files on tape

miscfile_plus
^^^^^^^^^^^^^

- added extra notations to avoid warnings in latest SQLAlchemy

fits_storage_config
^^^^^^^^^^^^^^^^^^^

- split magic api cookie into client/server versions, defaulting to the old single config value

api
^^^

- using server-side magic api cookie to validate incoming client requests


2021-2
======

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- Fix for datalabel links still having any obsid/datalabel terms present, making search too narrow

Scripts
-------

local_delete_files.py
^^^^^^^^^^^^^^^^^^^^^

- fix for detecting files on tape that are `.bz2`

delete_files.py
^^^^^^^^^^^^^^^

- fix to skip recent/export queues files but not abort

header_fixer2.py
^^^^^^^^^^^^^^^^

- Using z_staging_area when decompressing/compressing to fix files
- Fixing OBSERVAT keyword when corrupt in IGRINS datafiles

add_to_preview_queue.py
^^^^^^^^^^^^^^^^^^^^^^^

- Added a new `force` option to recreate previews even if they already exist [#387]

copy_from_dhs.py
^^^^^^^^^^^^^^^^

- Batching email for validation errors to not spam when first starting up [#391]
- No longer ignoring DHS files without a TELESCOP field in the header, these are legal

problem_checker.py
^^^^^^^^^^^^^^^^^^

- Added check for filesize difference over 10% between DHS and Dataflow [GL#8]

service_target_queue.py
^^^^^^^^^^^^^^^^^^^^^^^

- Properly detect location of Gemini North and South for target calculation, skip GPI [GL#5]

add_to_calcache_queue.py
^^^^^^^^^^^^^^^^^^^^^^^^

- Changed option to ignore_mdbad and accepting bad metadata inputs by default

Other
-----

exportqueue
^^^^^^^^^^^

- Alternate handling for >2GB files to work around limits in older python library ``urllib``

local_calibs
^^^^^^^^^^^^

- Major refactor to separate out calibration logic into GeminiCalMgr project

fits_storage.orm
^^^^^^^^^^^^^^^^

- Major refactor to separate out database logic into FitsStorageDB project

archive-httpd.conf
^^^^^^^^^^^^^^^^^^

- Updated log rotation to cap count and base on filesize

arcdev-httpd.conf
^^^^^^^^^^^^^^^^^

- Updated log rotation to cap count and base on filesize

httpd-patched-centos8.conf
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Updated log rotation to cap count and base on filesize


2021-1
======

Updated Web Services
--------------------

admin_change_email.py
^^^^^^^^^^^^^^^^^^^^^

- This also serves as a user list and I added a user_admin role to grant access without superuser [GL#2]

calmgr.py
^^^^^^^^^

- Now able to handle Section() objects passed from DRAGONS cal requests


2020-2.21
=========

Web Services
------------

tapestuff.py
^^^^^^^^^^^^

- bzip2 support

Scripts
-------

check_on_tape.py
^^^^^^^^^^^^^^^^

- bzip2 updates

delete_files.py
^^^^^^^^^^^^^^^

- Using new XML format and support for bzipped data

read_from_tape.py
^^^^^^^^^^^^^^^^^

- enhanced query options

request_from_tape.py
^^^^^^^^^^^^^^^^^^^^

- added --no-really option to override requirement for a --file-re or --tape-label

verify_tape.py
^^^^^^^^^^^^^^

- added --start-from option for an initial file number
- logfile suffix standardized handling
- bzip2 support

write_to_tape.py
^^^^^^^^^^^^^^^^

- order by ut_datetime

Other
-----

fits_store_config.py
^^^^^^^^^^^^^^^^^^^^

- Configuration records for tape server
- Configurable delete min age setting, set to 14 for tape server

fileontape.xml
^^^^^^^^^^^^^^

- Added data size/md5s and compression flag


2020-2.20
=========

Scripts
-------

run_new_photstandards.py
^^^^^^^^^^^^^^^^^^^^^^^^

- New script to assist in populating mappings for new standards without a full rerun


2020-2.19
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- added standard option to search for files with tags that include 'STANDARD'

Other
-----

fits_storage.orm
^^^^^^^^^^^^^^^^

- refactored all shared objects out into FitsStorageDB project as part of the calibration refactor

local_calibs
^^^^^^^^^^^^

- refactored all calibration code out into GeminiCalMgr to be a proper dependency of DRAGONS and share code

header.py
^^^^^^^^^

- updated header parsing for large values of airmass to take sec(90-elevation), if available, as an estimate

downloads
^^^^^^^^^

- fixed typo in "associated"


2020-2.18
=========

User-Facing Changes
-------------------

/searchform
^^^^^^^^^^^

- added calprog and notcalprog options to search explicitly for calibration programs or to exclude (URL only)
- highlight color changes for sq and ql data

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
- Added support for var, dq, sci (1d) headers in gmos validation

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

header.py
^^^^^^^^^

- Fix to pre_image header parsing for boolean values
- Fix to airmass parsing for bad string values like 'Unknown'


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

YouGotDataEmail.py
^^^^^^^^^^^^^^^^^^

- catch errors per notification and allow the rest to be tried, email full list of errors at end if any [GL#19]

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
========

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

- Detect when we have no END in the header and abort.  This prevents running out of memory on large corrupt Zorro files.

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

