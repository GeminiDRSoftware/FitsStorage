# FitsStorageCore

This project is the core of the Gemini Fits Storage data management system.
Along with the FitsStorage project, this is the software behind the 
Gemini Observatory Archive, and the Gemini FITS servers. This "Core" 
component provides the core of the data handling infrastructure and also
the calibration association system. This is used both in the FitsStorage
system, and also in the DRAGONS data reduction package, where it provides
calibration association.

## fits_storage_orm

This package provides a set of DB-backed classes to parse Gemini FITS
file metadata into various database tables. These tables, and this interface
layer to them, power almost all the functionality of the Fits Storage system.

The database backend can be anything supported by SQLAlchemy. In practice, we
use Postgresql for our server deployments, and SQLite when used within DRAGONS.

## fits_storage_cal

This package is the Fits Storage calibration association system.  This logic
is handled via per-instrument calibration definitions.  The matching rules are 
expressed as python code that constructs SQLAlchemy database queries against 
the Fits Storage database.


## Calibration Checking

When you have a calibration file that doesn't match a target file when you
expect it to, this library provides the tool `calcheck` for inspecting the
query for mismatches to diagnose it.  It does a best-effort job but can be
helpful.  You can supply `auto` to infer the calibration type, or you can
replace `auto` below with `flat`, `bias`, etc.

  Useage: calcheck <target file> auto <calibration file>

## DRAGONS Dependencies

There is a circular dependency in that this project depends on `DRAGONS` as we
use `astrodata` for all the FITS file access in Fits Storage, and `DRAGONS`
depends on this project to provide calibration association.


Two possible long-term solutions would be to break out the `astrodata` from
`DRAGONS` as a separate library, or to integrate this project directly
into `DRAGONS`.

