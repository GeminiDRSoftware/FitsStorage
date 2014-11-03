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
    fp = open(preview_fullpath, 'w')

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
    render_preview(diskfile.ad_object, fp)

    # Do any cleanup from above
    if our_dfado:
        diskfile.ad_object.close()
        if our_dfcc:
            os.unlink(ad_fullpath)

    # Now we should have a preview in fp. Close the file-object
    fp.close()

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
        xmax = 0
        ymax = 0
        ds = ad.detector_section().as_dict()
        for i in ds.values():
            [x1, x2, y1, y2] = i
            xmax = x2 if x2 > xmax else xmax
            ymax = y2 if y2 > ymax else ymax
    
        # Divide by binning
        xmax /= int(ad.detector_x_bin())
        ymax /= int(ad.detector_y_bin())
    
        # Make empty array for full image
        shape = (ymax, xmax)
        full = numpy.zeros(shape, ad['SCI', 1].data.dtype)
    
        # Loop through ads, pasting them in. Do gmos bias hack
        for add in ad['SCI']:
            s_xmin, s_xmax, s_ymin, s_ymax = add.data_section().as_pytype()
            d_xmin, d_xmax, d_ymin, d_ymax = add.detector_section().as_pytype()
            d_xmin /= int(ad.detector_x_bin())
            d_xmax /= int(ad.detector_x_bin())
            d_ymin /= int(ad.detector_y_bin())
            d_ymax /= int(ad.detector_y_bin())
            o_xmin, o_xmax, o_ymin, o_ymax = add.overscan_section().as_pytype()
            bias = numpy.median(add.data[o_ymin:o_ymax, o_xmin:o_xmax])
            gain = float(add.gain())
            full[d_ymin:d_ymax, d_xmin:d_xmax] = (add.data[s_ymin:s_ymax, s_xmin:s_xmax] - bias) * gain
    else:
        full = ad['SCI', 1].data
    
    # Normalize onto range 0:1 using percentiles
    plow = numpy.percentile(full, 0.3)
    phigh = numpy.percentile(full, 99.7)
    full = numpy.clip(full, plow, phigh)
    full -= plow
    full /= (phigh - plow)
    
    # plot without axes or frame
    fig = plt.figure(frameon=False)
    ax = plt.Axes(fig, [0, 0, 1, 1])
    ax.set_axis_off()
    fig.add_axes(ax)
    ax.imshow(full, cmap=plt.cm.gray)
    
    fig.savefig(outfile, format='jpg')

    plt.close()
