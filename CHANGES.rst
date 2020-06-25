2020-2 (unreleased)
===================

User-Facing Changes
----------------------

- Search by GPI Astrometric Standard [#253]
- Fixed time ranges to operate from 2pm-2pm local time in Chile/Hawaii [#288]
- Ability to search for calibrations associated with selected files [#356]

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

Updated Scripts
---------------

delete_files.py
^^^^^^^^^^^^^^^

- Configurable minimum age for files to delete, will skip any files with a filename pattern implying it's too new
- Error messages also added to a dedicated error email, which is sent only if errors were seen
- Checks export queue and skips files that are awaiting export

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- IGRINS checks for and adds RELEASE keyword if missing (and DATE-OBS exists)

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

