.. usage

Usage
=====

The `GeminiObsDB` package provides tools to store a database of
Gemini observation metadata.  This can be used standalone to
manage an `SQLite` file backed database, or it can be configured
to use a PostgreSQL Database Server.  If it is run without
any special configuration, it will default to using SQLite.

To modify the database settings, update the configuration fields
in `gemini_obs_db.db_config` to suit your needs.  In
particular, you may want to modify these settings

database_url
------------

This is the SQLAlchemy URL for the database to store your data
in.  You can use this to customize the SQLite database file location
or to specify any other SQLAlchemy URL.

z_staging_area
--------------

This is the path to a folder to use for uncompressing bzipped
FITS files.  It's not needed if you don't plan to operate on
compressed data.
