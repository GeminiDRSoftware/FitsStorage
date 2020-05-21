
2020-2 (unreleased)
===================

User-Facing Changes
----------------------

- Search by GPI Astrometric Standard [#253]
- Fixed time ranges to operate from 2pm-2pm local time in Chile/Hawaii [#288]

Service fixes and enhancements
------------------------------

fits_storage
^^^^^^^^^^^^

- Database backup location now calculated from hostname by default [#342]


Web Services
------------

/login
^^^^^^

- Now accepts a redirect query argument to invoke an http redirect on successful login


2020-1.1
========

New Web Services
----------------------

/publication/ads/<bibcode>
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Get an ADS record for a specific bibliography code

/list_publications
^^^^^^^^^^^^^^^^^^

- Get a list of all bibliography codes in the system

