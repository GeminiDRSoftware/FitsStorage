2023-02-03
==========

Re-merged the GeminiObsDB (aka FitsStorageDB) and GeminiCalMgr repositories that were previously
split out from the original FitsStorage repository into this one FitsStorageCore repository.
This allows a lot of simplification of configuration and other items.

This includes the following recent changes:

ghost

^^^^^
- Initial support for GHOST in ORM mappings, with per arm descriptors where neede


gemini_metadata_utils
^^^^^^^^^^^^^^^^^^^^^

- Support for Z date/time suffix to specify UTC
- Fix for obsid parsing that caused metadata checks to incorrectly pass
- Fix for ra/dec parsing to handle additional strings
- Allowing for - delimited extensions like QL-FLAT
- using astropy for string parsing of RA/DEC coordinates
- fix parsing of observation number from new regex
- fix for parsing of datalabels with updated regex
- fix for checking for a datetime string when not handling a range
- Support for YYYYMMDDTHHMMSS-YYYYMMDDTHHMMSS style datetime ranges for SCALeS [#4]
- Support for new-style non-site-specific program IDs [#1]


file_parser
^^^^^^^^^^^

- Fix disperser to parse with pretty=True
- Handle files with missing tags cleanly
- Using lambda for descriptor accesses so they happen inside the _try_or_none error handler
- Fixes to failure logging to skip the log if we were not provided with a log instance
- WCS first, then fallback to repair WCS, then fallback to ra() or dec()
- smarter about unexpected text format RA
- error reporting for really bad RA/DEC values
- Fix for bad telescope values from IGRINS [#2]
- Properly detect IGRINS files and use the correct parser (IGRINS parser can handle uncorrected IGRINS files) [#3]
- Converting non-string values in program IDs


gmos
^^^^
- Adding array_name parameter to facilitate BPM matching logic


gemini_obs_db
^^^^^^^^^^^^^

- added pre-ping for Postgres connections so we don't use an expired connection


calibration
^^^^^^^^^^^

- Include camera descriptor to support GHOST calibration rules
- support for all instruments for calibration debugging
- support for BPMs
- support for QH* gcal lamps
- using literal_eval for types parsing to be safe from malicious data [#2]


calibration_ghost
^^^^^^^^^^^^^^^^^

- Initial calibration class for GHOST
- Keying off reduction type PROCESSED_UNKNOWN as that is how the reduction is set in the Header table


calibration_f2
^^^^^^^^^^^^^^

- read_mode not needed to match for arcs and flats (checked with Joan)


associate_calibrations
^^^^^^^^^^^^^^^^^^^^^^
- do final sort to bubble up any BPMs to the top of the list


calcheck
^^^^^^^^
- fixes to the command line tool for checking calibrations

