#!/usr/bin/python

"""
Begin by ingesting file(s) from disk and reading.
Retrieve numpy data array from fits file via astrodata.
Convert numpy array to 2D image representation using matplotlib.
Convert to and save as jpeg.
"""

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from astrodata import AstroData

import numpy as np

def getfits():
    """
    get fits files from database directly
    read their numpy arrays
    """
    
    """
    # Figure it out later - trying to read in files from database, probably pass this function a string (ie. user-selected fits)
    filename = '/net/wikiwiki/dataflow/'
    session = sessionfactory()

    query = session.query(DiskFile.filename)
    query = query.filter(Header.instrument == 'GNIRS')
    query = query.filter(Header.ut_datetime >= datetime.date(2013, 6, 30))
    
    for file in query:
        readarray(file)
    """

    # hacked in to be an arbitrary GMOS file at the moment
    readarray('N20130815S0378.fits')

def readarray(file):
    """
    Receive fits file and interpret image array data as necessary
    Sends to normalizing, stitching, etc. functions respectively
    """

    # declare astrodata object
    adfile = AstroData('/net/wikiwiki/dataflow/' + file)
    
    # stitch arrays if more than one is present; otherwise, simply extract the one array
    if len(adfile) > 1:
        fitsarray = stitcher(adfile)
    else:
        fitsarray = adfile.data
    
    # normalize the array (makes it easier to work with in matplotlib), and convert it to grayscale matplotlib image
    fitsarraynorm = norm(fitsarray)
    fitsplot = plt.imshow(fitsarraynorm, cmap=plt.cm.gray)

def norm(fitsarray, high=1.0, low=0.0):
    """
    Rough and ready normalizer for normalizing the data array
    Set normalization bounds in function parameters
    """
    
    #declare max, min, and total range of array
    amin = np.min(fitsarray)
    amax = np.max(fitsarray)
    rng = amax - amin
    
    # normalize array
    offset = fitsarray + np.fabs(amin)
    norm = (high - low) * (offset / rng)
    return norm

def stitcher(adfile):
    """
    stitch together multiple arrays if necessary; currently only for GMOS fits files
    """
    
    if 'GMOS' in str(adfile.instrument()):
        # make empty lists for populating
        # should work for any instrument, independent of number of arrays present
        # detector_section must contain pixel data, and its second element must be the "x2" value
        xmaxlist = []
        ordering = []
        arraylist = []

        for arr in adfile:
            # populates a list with the max pixel values on the x-axis for each array
            xmax = arr.detector_section().as_pytype()[1]
            xmaxlist.append(xmax)

        for num in xrange(len(xmaxlist)):
            # populates a list with id numbers in ascending order of x-axis pixel value, where id is astrodata[("SCI", id)]
            id = [i for i,x in enumerate(xmaxlist) if x == np.min(xmaxlist)][0]
            ordering.append(id + 1)
            xmaxlist[id] = np.max(xmaxlist) + 1

        for id in ordering:
            # puts image arrays in correct order into a list for stitching, indexing against list 'ordering' from above
            arraylist.append(adfile[("SCI",id)].data)

        stitched_array = np.hstack(arraylist)
        return stitched_array
    else:
        pass
        
getfits()
plt.show()
plt.savefig('fits1.jpg')
