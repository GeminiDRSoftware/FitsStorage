
Updated Web Services
--------------------

/summary
^^^^^^^^

- Now sorts null ut datetimes to the end of the results, so most recent properly displays
- Removed engineering results by default

Updated Scripts
---------------

exportqueue.py
^^^^^^^^^^^^^^

- During export, increased timeout of file post to 10 minutes as Zorro files take time to upload

copy_from_visiting_instrument.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Added compression option which, if set to false, does not re-encrypt data after fixing the headers

2020-1.5
========

Updated Scripts
---------------

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

