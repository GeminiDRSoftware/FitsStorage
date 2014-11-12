"""
This module provides various utility functions to
manage and service the preview queue
"""
import os
import datetime
from logger import logger
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

from orm.preview import Preview
from orm.previewqueue import PreviewQueue

from fits_storage_config import using_s3, storage_root, preview_path, s3_staging_area, z_staging_area
import bz2

from astrodata import AstroData
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def pop_previewqueue(session):
    """
    Returns the next thing to ingest off the queue, and sets the
    inprogress flag on that entry.

    The ORM instance returned is detached from the database - it's a transient
    object not associated with the session. Basicaly treat it as a convenience
    dictionary for the diskfile_id etc, but don't try to modify the database with it.

    The select and update inprogress are done with a transaction lock
    to avoid race conditions or duplications when there is more than
    one process processing the ingest queue.

    The queue is ordered by the sortkey, which is the filename

    Also, when we go inprogress on an entry in the queue, we
    delete all other entries for the same filename.
    """

    # Is there a way to avoid the ACCESS EXCLUSIVE lock, especially with 
    # fast_rebuild where we are not changing other columns. Seemed like
    # SELECT FOR UPDATE ought to be able to do this, but it doesn't quite
    # do what we want as other threads can still select that row?

    session.execute("LOCK TABLE previewqueue IN ACCESS EXCLUSIVE MODE;")

    query = session.query(PreviewQueue).filter(PreviewQueue.inprogress == False)
    query = query.order_by(desc(PreviewQueue.sortkey))

    pq = query.first()
    if pq is None:
        logger.debug("No item to pop on preview queue")
    else:
        # OK, we got a viable item, set it to inprogress and return it.
        logger.debug("Popped id %d from preview queue", pq.id)
        # Set this entry to in progres and flush to the DB.
        pq.inprogress = True
        session.flush()

        # Make the pq into a transient instance before we return it
        # This detaches it from the session, basically it becomes a convenience container for the
        # values (diskfile_id, etc). The problem is that if it's still attached to the session
        # but expired (because we did a commit) then the next reference to it will initiate a transaction
        # and a SELECT to refresh the values, and that transaction will then hold a FOR ACCESS SHARE lock
        # on the exportqueue table until we complete the export and do a commit - which will prevent
        # the ACCESS EXCLUSIVE lock in pop_exportqueue from being granted until the transfer completes.
        make_transient(pq)

    # And we're done, commit the transaction and release the update lock
    session.commit()
    return pq

def previewqueue_length(session):
    """
    return the length of the preview queue
    """
    length = session.query(PreviewQueue).filter(PreviewQueue.inprogress == False).count()
    # Even though there's nothing to commit, close the transaction
    session.commit()
    return length


def make_preview(session, diskfile):
    """
    Make the preview, given the diskfile.
    This is called from within service_ingest_queue ingest_file
    - it will use the pre-fetched / pre-decompressed / pre-opened astrodata object if possible
    - the diskfile object should contain an ad_object member which is an AstroData instance
    """

    # Setup the preview file
    preview_filename = diskfile.filename + "_preview.jpg"
    if using_s3:
        # Create the file in s3_staging_area
        preview_fullpath = os.path.join(s3_staging_area, preview_filename)
    else:
        # Create the preview filename 
        preview_fullpath = os.path.join(storage_root, preview_path, preview_filename)

    # render the preview jpg
    # OK, for now, we only implement the case where either the diskfile.ad_object exists
    # or the diskfile represents a local file
    our_dfado = diskfile.ad_object == None
    our_dfcc = False
    if our_dfado:
        if using_s3:
            logger.error("This kind of preview build is not yet supported")
            return
        else:
            if diskfile.compressed:
                # Create the uncompressed cache filename and unzip to it
                nonzfilename = diskfile.filename[:-4]
                diskfile.uncompressed_cache_file = os.path.join(z_staging_area, nonzfilename)
                if os.path.exists(diskfile.uncompressed_cache_file):
                    os.unlink(diskfile.uncompressed_cache_file)
                in_file = bz2.BZ2File(diskfile.fullpath(), mode='rb')
                out_file = open(diskfile.uncompressed_cache_file, 'w')
                out_file.write(in_file.read())
                in_file.close()
                out_file.close()
                our_dfcc = True
                ad_fullpath = diskfile.uncompressed_cache_file
            else:
                # Just use the diskfile fullpath
                ad_fullpath = diskfile.fullpath()
            # Open the astrodata instance
            diskfile.ad_object = AstroData(ad_fullpath)

    # Now there should be a diskfile.ad_object, either way...
    fp = open(preview_fullpath, 'w')
    try:
        render_preview(diskfile.ad_object, fp)
        fp.close()
    except:
        os.unlink(preview_fullpath)
        raise

    # Do any cleanup from above
    if our_dfado:
        diskfile.ad_object.close()
        if our_dfcc:
            os.unlink(ad_fullpath)

    # Now we should have a preview in fp. Close the file-object

    # If we're not using S3, that's it, the file is in place.
    # If we are using s3, need to upload it now.
    if using_s3:
        logger.debug("Connecting to S3")
        s3conn = S3Connection(aws_access_key, aws_secret_key)
        bucket = s3conn.get_bucket(s3_bucket_name)
        k = Key(bucket)
        k.key = preview_filename
        logger.info("Uploading %s to S3 as %s" % (preview_fullpath, preview_filename))
        k.set_contents_from_filename(preview_fullpath)
        os.unlink(src)


    # Add to preview table
    preview = Preview(diskfile, preview_filename)
    session.add(preview)

    
def render_preview(ad, outfile):
    """
    Pass in an astrodata object and a file-like outfile.
    This function will create a jpeg rendering of the ad object
    and write it to the outfile
    """

    if 'GMOS' in str(ad.instrument()):
        # Find max extent in detector pixels
        xmin = 10000
        ymin = 10000
        xmax = 0
        ymax = 0
        ds = ad.detector_section().as_dict()
        for i in ds.values():
            [x1, x2, y1, y2] = i
            xmin = x1 if x1 < xmin else xmin
            ymin = y1 if y1 < ymin else ymin
            xmax = x2 if x2 > xmax else xmax
            ymax = y2 if y2 > ymax else ymax

        # Divide by binning
        xmin /= int(ad.detector_x_bin())
        ymin /= int(ad.detector_y_bin())
        xmax /= int(ad.detector_x_bin())
        ymax /= int(ad.detector_y_bin())
    
        logger.debug("Full Image extent is: %d:%d, %d:%d", xmin, xmax, ymin, ymax)

        # Make empty array for full image
        gap = 40 # approx chip gap in pixels
        shape = (ymax-ymin, (xmax-xmin)+2*gap)
        full = numpy.zeros(shape, ad['SCI', 1].data.dtype)
    
        # Loop through ads, pasting them in. Do gmos bias and gain hack
        for add in ad['SCI']:
            s_xmin, s_xmax, s_ymin, s_ymax = add.data_section().as_pytype()
            logger.debug("Source Image extent is: %d:%d, %d:%d", s_xmin, s_xmax, s_ymin, s_ymax)
            d_xmin, d_xmax, d_ymin, d_ymax = add.detector_section().as_pytype()
            # Figure out which chip we are and add gap padding
            # All the gmos chips ever have been 2048 pixels in X.
            if d_xmin == 4096 or d_xmin == 5120:
                pad = 2*gap
            elif d_xmin == 2048 or d_xmin == 3072:
                pad = gap
            else:
                pad = 0
            
            d_xmin += pad
            d_xmax += pad
            d_xmin /= int(ad.detector_x_bin())
            d_xmax /= int(ad.detector_x_bin())
            d_ymin /= int(ad.detector_y_bin())
            d_ymax /= int(ad.detector_y_bin())
            d_xmin -= xmin
            d_xmax -= xmin
            d_ymin -= ymin
            d_ymax -= ymin
            o_xmin, o_xmax, o_ymin, o_ymax = add.overscan_section().as_pytype()
            bias = numpy.median(add.data[o_ymin:o_ymax, o_xmin:o_xmax])
            gain = float(add.gain())
            logger.debug("Pasting: %d:%d,%d:%d -> %d:%d,%d:%d", s_xmin, s_xmax, s_ymin, s_ymax, d_xmin, d_xmax, d_ymin, d_ymax)
            full[d_ymin:d_ymax, d_xmin:d_xmax] = (add.data[s_ymin:s_ymax, s_xmin:s_xmax] - bias) * gain

        full = norm(full)
           

    elif str(ad.instrument()) == 'GSAOI':
        gap = 125
        size = 4096 + gap
        shape = (size, size)
        full = numpy.zeros(shape, ad['SCI', 1].data.dtype)
        # Loop though ads, paste them in
        for add in ad['SCI']:
            [x1, x2, y1, y2] = add.detector_section().as_pytype()
            xoffset = 0 if x1 == 0 else gap
            yoffset = 0 if y1 == 0 else gap
            logger.debug("x1 x2 y1 y2: %d %d %d %d", x1, x2, y1, y2)
            logger.debug("xoffset yoffset", xoffset, yoffset)
            logger.debug("full shape: %s", full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset].shape)
            logger.debug("data shape: %s", add.data.shape)
            full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset] = add.data

        full = norm(full)

    elif str(ad.instrument()) in ['TReCS', 'michelle']:
        # We just preview the first extension for now.
        sciext = 1
        # Just sum up along the 4th axis. Ahem, this is the 0th axis in numpy land
        data = ad['SCI', sciext].data
        data = numpy.sum(data, axis=0)
        # Now the new 0th axis is the chop position
        # If it's two long, subtract the two, otherwise just go with first plane
        chop_a = None
        chop_b = None
        if data.shape[0] == 2:
            chop_a = data[0,:,:]
            chop_b = data[1,:,:]
            data = chop_a - chop_b
        elif data.shape[0] == 3:
            chop_a = data[1,:,:]
            chop_b = data[2,:,:]
            data = data[0,:,:]
        else:
            data = data[0,:,:]

        data = norm(data)
        if chop_a is not None and chop_b is not None:
            # Make a tile version
            chop_a = norm(chop_a)
            chop_b = norm(chop_b)
            full_shape = (500, 660)
            full = numpy.zeros(full_shape, data.dtype)
            full[260:500, 0:320] = chop_a
            full[260:500, 340:660] = chop_b
            full[0:240, 180:500] = data
        else:
            # Just paste in the data
            full = data
    else:
        # Generic plot the first extention case
        full = ad['SCI', 1].data

        # Do a numpy squeeze on it - this collapses any axis with 1-pixel extent
        full = numpy.squeeze(full)

        full = norm(full)
    
    # plot without axes or frame
    fig = plt.figure(frameon=False)
    ax = plt.Axes(fig, [0, 0, 1, 1])
    ax.set_axis_off()
    fig.add_axes(ax)
    ax.imshow(full, cmap=plt.cm.hot)
    
    fig.savefig(outfile, format='jpg')

    plt.close()

def norm(data, percentile=0.3):
    """
    Normalize the data onto 0:1 using percentiles
    """
    lower = percentile
    upper = 100.0 - percentile
    plow = numpy.percentile(data, lower)
    phigh = numpy.percentile(data, upper)
    data = numpy.clip(data, plow, phigh)
    data -= plow
    data /= (phigh - plow)
    return data 
