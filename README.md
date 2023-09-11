# FITSStorage

FITSStorage is a python package developed at the Gemini Observatory to help with
a number of data management, data distribution and data calibration tasks.

The core of fits_storage is a database ORM layer that describes FITS files and
harvests some information from the FITS header. We use SQLAlchemy as the
database interface, and typically the code is used with either sqlite3 or
a postgresql database backend.

Also included is the "calibration system" which facilitates calibration
association. The model is that you reference a science data file and say "find
me a flat field for this" and the system will construct and execute a database
query that should select the mode optimal flat field known for the science file
in question. In practice of course, the "science file" can be anything that
needs a calibration - so the system can find you a flat field to use to flat
correct an arc file for example.

The core functionality and calibration system are distributed in a python
package that is a dependency of the DRAGONS data reduction system, so as to
provide calibration association functionality in the DRAGONS pipelines.

The code also contains modules that provide a wsgi interface that can query
the database and provide both human-readable web pages and http(s) APIs. This
functionality is used to provide both internal data management tools at Gemini
and also the Gemini Observatory Archive (archive.gemini.edu). Also in the 
module are a number of scripts that are used to ingest files into the database
and provide housekeeping tasks.

The code uses the DRAGONS Astrodata classes for all access to the internals of
FITS files.

## Authors

* **Paul Hirst** - *Initial work and ongoing development*
* **Ricardo Cardenes** - *Initial work*
* **Ken Anderson** - *Initial work*
* **Oliver Oberdorf** - *2020 Python 3 based release and CI/CD*

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

