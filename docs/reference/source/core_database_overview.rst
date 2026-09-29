**********************
Core Database Overview
**********************

.. py:currentmodule:: fits_storage.core.orm

The FitsStorage database table can effectively be divided into several groups,
some of which are only relevant to certain configurations or deployments.
There is however a core set of tables, which are described here.

file
====

The file table is a very simple list of (human readable) files. The concept here
is that the actual filename in the storage system may differ from this (eg it
may have a `.bz2` suffix if it is compressed with bzip2), so this table provides
a simple top level list of files known to the system.

It has the following columns:

id
  Integer primary key. This is used as a foreign key in the diskfile table.

name
  Name of the file

diskfile
========

A row in the diskfile table represents a version of a file that (at least at one
point) was stored on disk. It contains metadata regarding the file and some
boolen status values. It contains a reference to the `file` table so that we
can recognize all diskfile instances that correspond to the file `file`.

When a file on disk is updated and re-ingested into the database, we add a new
row in the diskfile table for the new version of the file, we do not delete the
old row. There are two boolean status values for each file, `present` and
`canonical` which are updates in this case - see below for details.

All files stored in the database have a diskfile entry. While typically most
of the files are FITS files, they are not all - miscfiles, obslogs, etc all
have diskfile entries too (see the isfits column below)

Notable diskfile columns include:

id
  Integer primary key. This is used as a foriegn key in several other tables

file_id
  Foreign key reference to a row in the `file` table. There is a 1:N relation
  between file and diskfile.

filename
  The file name of the file on disk

path
  Path within the storage root to the file on disk. We support (sometimes
  clumsily) multiple files with the same filename in different paths.

present
  A Boolean value saying if this version of the file is present on disk. When
  a version of a file is ingested into the database, it must be present on disk
  and the row is created with `present=True'. If a file is updated and a new
  version ingested, the old version is set to present=False. If a file is
  deleted from the disk, we set this to False. Only one row describing the same
  path/filename combination can be marked as present=True.

canonical
  A Boolean value saying if this is the canonical (best, most recent) version
  of the file. Generally this matches `present` excapt in cases where a file
  is deleted from disk and not replaced by an updated version of the file (for
  example in the case of deleting old data to free up disk space). In that case
  canonical is left set at True, so that we can continue to use the record of
  the file for things like calibration association, while knowing that is it
  no longer present on disk.

file_size
  The size of the file on disk in bytes.

file_md5
  The md5 sum of the file on disk

compressed
  A boolean saying if the file is compressed (typically with bzip2)

data_size
  If the file is compressed, this is the size in bytes of the uncompressed data.
  If the file is not compressed, it will be equal to file_size

data_md5
  If the file is compressedm this is the md5 sum of the uncompressed data. If
  the file is not compressed, it will be equal to file_md5

lastmod
  The last modficiation timestamp of the file from the filesystem when the file
  was ingested

entrytime
  The timestamp when this database entry was created (ie when this version of
  this file was ingested into the database)

isfits
  A Boolean indicating if this file is a (possibly compressed) fits file.


header
======

The header table is in many ways the real core of the fits storage database. It
is somewhat of a misnomer in that the table does not store raw header values,
rather the values stored here are primarily the values returned by astrodata
descriptors.

The header table is the main (but not only) table searched to respond to
searchform queries on the database. It contains human-readable details of the
metadata from the fits file (ie generally `pretty=True` is passed to the
astrodata descriptor when populating header. The values here can be used in
calibration association, though this instrument specific (not part of the core
database) tables will often contain values more appropriate for calibation
association - for example the component ID numbers are stripped off values in
header but not in the instrument specific table.

Rows are created in header for all FITS diskfiles in
the database. Rows are never deleted. Almost all queries against header will
want to join against diskfile and query on diskfile.canonical being True.

Columns in the header table are intended to be generic in the
sense of not being instrument specific. However it is true that some columns are
not especially meaningful to all instruments - all entries for instruments that
do not support detector binning for example will show '1x1` binning.

Some of the columns in header are enumerated types. This makes database storage
and searching for string values when there are a very limited and fixed set of
possible values more efficient, at the penalty of needing to manually update the
enumerated type in the event that an additional value does arrise. For example
we enumerate the telescope value where the only possible values are Gemini-North
or Gemini-South, but we do not enumerate the instrument value, as we do get
new instruments from time to time.

The table has many columns, a subset are described here, omitting many of the
less notable values:

id
  Integer primary key. This is used as a foreign key in several other tables

diskfile_id
  Foreign key to the diskfile table. In principle there is a 1:N relationship
  between diskfile and header, though in practice N is always 1.

telescope
  Enumerated telescope name string

instrument
  String name of the instrument

ut_datetime
  UTC Date-Time of the observation

program_id
  Gemini Program ID of the observation. Similar columns exist for observation
  id and data label

observation_class
  Gemini OBSCLASS value.

observation_type
  Gemini OBSTYPE value. There are a few cases where we assign new values
  based on other metadata where the raw data obstype is not really suitable.

filter_name
  Filter name (from the astrodata filter_name(pretty=True) descriptor

release
  Release date for the data to become public


footprint
=========

The footprint table contains descriptions of the geometric footprint of an
observation on the sky. It is used to search for data which covers a specific
sky location. It used postgres geometic types to support this. The current
implementation does not use pgSphere and some approximations are made which will
not be strictly correct near the poles for example.

It contains a header_id column which is a 1:N foreign key reference to the
header table. There is one row in header for each extension of the fits file
referenced in header.

diskfilereport
==============

The diskfile report table is essentially a 1:1 continuation of the diskfile
table, containing large text fields that would affect performance of diskfile
searches if combined into the diskfile table.

It contains human readable text
from the fitsverify program and from our own metadata verification code. These
are used operationally to find and fix metadata issues.

fulltextheader
==============

The full text header table is another 1:1 continuation of the diskfile
table, containing large text fields that would affect performance of diskfile
searches if combined into the diskfile table.

It contains a human readable dump of the complete raw fits header from the
file. This is primarily used for the header display function in the web
interface.


