# Some notes on development environments...

The test code references some environment variables for test data locations
etc. These are the relevant ones I have set:

`PYTHONPATH`: Put the directory where you checked out FitsStorage, and also the 
one where you checked out DRAGONS on your PYTHONPATH if you're using a local 
checkout of DRAGONS.

`DRAGONS_TEST`: If you want to run the DRAGONS pytests, point this at where the
test data lives.

`FITS_STORAGE_TEST_DATA`: The Fits Storage tests will check this directory for
test data. If it's not set, or the file is not there, they will just download
the data from the archive. The tests don't populate this directory though, you
have to do that manually if you want to not download from the archive each time.

Here's a list of the files I have in that directory. You can get these using
wget or curl, from https://archive.gemini.edu/file/<filename> 

```
N20100119S0080.fits.bz2
N20150505S0119.fits.bz2
N20180329S0134.fits.bz2
N20180524S0117.fits.bz2
N20191002S0080.fits.bz2
N20200127S0023.fits.bz2
N20240531S0144.fits.bz2
S20130123S0131.fits.bz2
S20130124S0036.fits.bz2
S20171125S0116.fits.bz2
S20181018S0151.fits.bz2
S20181219S0333.fits.bz2
S20240320S0027.fits.bz2
bpm_20220303_gmos-n_Ham_44_full_12amp.fits.bz2
```

`FITS_STORAGE_TEST_LIVESERVER` is the base URL for the "liveserver" tests, 
for example=https://archive.gemini.edu or http://arcdev.gemini.edu


`FITS_STORAGE_TEST_TESTSERVER` is the base URL for the "testserver" tests,
for example http://localhost:8000

`PATH`: you may (or may not) want to add the `fits_storage/scripts` directory
to your shell's execution search path.