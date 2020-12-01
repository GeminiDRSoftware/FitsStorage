2020-2.6
========

/gmoscalbiasfiles
^^^^^^^^^^^^^

- Made bias file list bugfixes [#398]


2020-2.5
========

User-Facing Changes

/searchform
^^^^^^^^^^^

- Added B480 grating to GMOS searches


2020-2.4
========

Uesr-Facing Changes
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

