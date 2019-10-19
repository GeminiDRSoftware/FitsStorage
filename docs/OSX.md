# OSX Install Instructions

## Requirements

### PostgreSQL

You will need a PostgreSQL server.  Personally, I prefer `Postgres.app`.  This way I can run it on demand and leave it 
off if I am not doing development on the website.

https://postgresapp.com/

Once setup, you will want to create a `fitsdata` database.

### fitsverify

The ingest process makes use of `fitsverify`.

First, you will need to install cfitsio, if you do not already have it.  This can be done a variety of ways, as outlined
on their website here:

https://heasarc.gsfc.nasa.gov/fitsio/fitsio_macosx.html

I have homebrew installed on my Mac, so for me it was as simple as:

`brew install cfitsio`

Next, you need to install `fitsverify`.  Unfortunately, this is available only as source.  You can copy the `fitsverify`
folder out of the `FitsStorage` repo to somewhere convenient.  I used `~/fitsverify`.

To compile, you have to modify the command they suggest to leave off `-lnl` and `-lsocket`.  Here is the command I used.

`gcc -o fitsverify ftverify.c fvrf_data.c fvrf_file.c fvrf_head.c fvrf_key.c \
    fvrf_misc.c -DSTANDALONE -L. -lcfitsio -lm -lnsl -lsocket`

Note that it spits out some warnings, but it does build the binary.

### Anaconda

I installed Anaconda3.  You probably already have this, but if not just go get it here:

https://www.anaconda.com/

I like to create a custom environment for running things.

`conda env create <my_environment_name>`

### Python Packages

Unfortunately, I don't yet have a fully working .yml spec for OSX.  The one that I do have for Jenkins causes
some nasty infinite loop if you try and use it on OSX.  So, for now, I would pip install these packages, as
indicated in `.jenkins/conda_py3env_stable.yml`:

  - astropy
  - coverage
  - future
  - pip
  - pytest
  - pylint
  - requests
  - scipy
  - sphinx_rtd_theme
  - sqlalchemy
  - psycopg2
  - pyyaml
  - jinja2
  - cefca::pyfits
  - conda-forge::dateutils
  - pandas
  - boto3
  - matplotlib

### DRAGONS

In addition to the FitsStorage codebase, you need to have a copy of DRAGONS.  You can check this out of git.
You may want to clone via ssh instead, depending on your permissions.

https://github.com/GeminiDRSoftware/DRAGONS

`git clone https://github.com/GeminiDRSoftware/DRAGONS.git`

After checkout, you'll want to put it in your PYTHONPATH.  This can be done in your PyCharm run config
or in your .bash_profile with

`export PYTHONPATH=/where/you/put/DRAGONS`

### Data

You'll want to start out with some data.  I recommend just doing a search on archive.gemini.edu and grabbing
some public data.  Put this in a folder somewhere for later use.

### Logging Directory

You need a folder for your logs.  Just create a folder wherever and remember it's location

### Environment Variables

In order to override various default locations that FitsStorage uses on the servers, I have made a set of
environment variables.  You can set these variables to tell the FitsStorage scripts and website to go to
your folders instead of the usual locations.  Again, this could just be added to your .bash_profile or
you can put these in your IDE of choice.

```
export FITS_LOG_DIR=/where/you/put/logs/directory/
export STORAGE_ROOT=/where/you/put/the/data/
export FITS_AUX_DATADIR=/path/to/FitsStorage/data/
export HTML_DOC_ROOT=/path/to/FitsStorage/htmldocroot/
export FITSVERIFY_BIN=/path/to/fitsverify # <-- the binary, not a folder
export VALIDATION_DEF_PATH=/path/to/FitsStorage/docs/dataDefinition/
```

### Initialize Database

Before running anything, you'll want to setup your empty database with the schema.  To do this, you can
run the create_tables.py script in the `FitsStorage` repo.

`python fits_storage\scripts\create_tables.py`

### Queue Data Ingest

Now that the tables are in place, you'll want to add all your data files to the queue to be ingested.
Again, this is in the scripts directory.

`python fits_storage\scripts\add_to_ingest_queue.py`

### Run Ingest Service

Normally, the ingest service runs continuously.  You could start it up in a separate terminal if you
like, but it is also possible to run it like a one-off command using the `--empty` flag.

`python fits_storage\scripts\service_ingest_queue.py --empty`




