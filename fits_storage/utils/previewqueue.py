"""
This module provides various utility functions to
manage and service the preview queue
"""
import os
import datetime
from sqlalchemy import desc
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.orm import make_transient

from ..orm.diskfile import DiskFile
from ..orm.preview import Preview
from ..orm.previewqueue import PreviewQueue

from . import queue
import functools

from ..fits_storage_config import using_s3, storage_root, preview_path, z_staging_area
import bz2

if using_s3:
    from ..fits_storage_config import s3_staging_area
    from aws_s3 import get_helper

from astrodata import AstroData
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

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

class PreviewQueueUtil(object):
    def __init__(self, session, logger):
        self.s = session
        self.l = logger
        if using_s3:
            self.s3 = get_helper(logger_ = logger)

    def length(self):
        return queue.queue_length(PreviewQueue, self.s)

    def pop(self):
        return queue.pop_queue(PreviewQueue, self.s, self.l)

    def set_error(self, trans, exc_type, exc_value, tb):
        "Sets an error message to a transient object"
        queue.set_error(PreviewQueue, trans.id, exc_type, exc_value, tb, self.s)

    def delete(self, trans):
        "Deletes a transient object"
        queue.delete_with_id(PreviewQueue, trans.id, self.s)

    def process(self, diskfiles, make=False):
        try:
            iter(diskfiles)
        except TypeError:
            # Didn't get an iterable; Assume we were passed a single diskfile or previewqueue
            diskfiles = (diskfiles,)

        if make:
            # Go ahead and make the preview now
            for df in diskfiles:
                if isinstance(df, PreviewQueue):
                    pq = df
                    df = self.s.query(DiskFile).get(pq.diskfile_id)
                    self.l.info("Making Preview for {}: {}".format(pq.id, df.filename))
                else:
                    self.l.info("Making Preview with diskfile_id {}".format(df.id))
                if df.present == True:
                    self.make_preview(df)
                else:
                    self.l.info("Skipping non-present diskfile_id %d", df.id)
        else:
            # Add it to the preview queue
            for df in diskfiles:
                self.l.info("Adding PreviewQueue with diskfile_id {}".format(df.id))
                pq = PreviewQueue(df)
                self.s.add(pq)
            self.s.commit()

    def make_preview(self, diskfile):
        """
        Make the preview, given the diskfile.
        This can be called from within service_ingest_queue ingest_file, in which case
        - it will use the pre-fetched / pre-decompressed / pre-opened astrodata object if possible
        - the diskfile object should contain an ad_object member which is an AstroData instance
        It can also be called by service_preview_queue in which case we won't have that so it will
        then either open the file or fetch it from S3 as appropriate etc.
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
        # Are we responsible for creating an AstroData instance, or is there one for us?
        our_dfado = diskfile.ad_object == None
        our_dfcc = False
        try:
            if our_dfado:
                # We munge the filenames here to avoid file collisions in the staging directories
                # with the ingest process
                munged_filename = diskfile.filename
                munged_fullpath = diskfile.fullpath()
                if using_s3:
                    # Fetch from S3 to staging area
                    # TODO: We're not checking here if the file was actually retrieved...
                    munged_filename = "preview_" + diskfile.filename
                    munged_fullpath = os.path.join(s3_staging_area, munged_filename)
                    self.s3.fetch_to_staging(diskfile.filename, fullpath=munged_fullpath)

                if diskfile.compressed:
                    # Create the uncompressed cache filename and unzip to it
                    nonzfilename = munged_filename[:-4]
                    diskfile.uncompressed_cache_file = os.path.join(z_staging_area, nonzfilename)
                    if os.path.exists(diskfile.uncompressed_cache_file):
                        os.unlink(diskfile.uncompressed_cache_file)
                    with bz2.BZ2File(munged_fullpath, mode='rb') as in_file, open(diskfile.uncompressed_cache_file, 'w') as out_file:
                        out_file.write(in_file.read())
                    our_dfcc = True
                    ad_fullpath = diskfile.uncompressed_cache_file
                else:
                    # Just use the diskfile fullpath
                    ad_fullpath = munged_fullpath
                # Open the astrodata instance
                diskfile.ad_object = AstroData(ad_fullpath)

            # Now there should be a diskfile.ad_object, either way...
            with open(preview_fullpath, 'w') as fp:
                try:
                    self.render_preview(diskfile.ad_object, fp)
                except:
                    os.unlink(preview_fullpath)
                    raise

        finally:
            # Do any cleanup from above
            if our_dfado:
                if diskfile.ad_object is not None:
                    diskfile.ad_object.close()
                if using_s3:
                    os.unlink(munged_fullpath)
                if our_dfcc:
                    os.unlink(ad_fullpath)

        # Now we should have a preview in fp. Close the file-object

        # If we're not using S3, that's it, the file is in place.
        # If we are using s3, need to upload it now.
        if using_s3:
            self.s3.upload_file(preview_filename, preview_fullpath)
            os.unlink(preview_fullpath)

        # Add to preview table
        preview = Preview(diskfile, preview_filename)
        self.s.add(preview)


    def render_preview(self, ad, outfile):
        """
        Pass in an astrodata object and a file-like outfile.
        This function will create a jpeg rendering of the ad object
        and write it to the outfile
        """

        if 'GMOS' in str(ad.instrument()) and 'PROCESSED_SCIENCE' not in ad.types:
            # Find max extent in detector pixels
            xmin = 10000
            ymin = 10000
            xmax = 0
            ymax = 0
            ds = ad.detector_section().as_dict()
            for i in ds.values():
                [x1, x2, y1, y2] = i
                xmin, ymin = min(x1, xmin), min(y1, ymin)
                xmax, ymax = max(x2, xmax), max(y2, ymax)

            # Divide by binning
            xmin /= int(ad.detector_x_bin())
            ymin /= int(ad.detector_y_bin())
            xmax /= int(ad.detector_x_bin())
            ymax /= int(ad.detector_y_bin())

            self.l.debug("Full Image extent is: %d:%d, %d:%d", xmin, xmax, ymin, ymax)

            # Make empty array for full image
            gap = 40 # approx chip gap in pixels
            shape = (ymax-ymin, (xmax-xmin)+2*gap)
            full = numpy.zeros(shape, ad['SCI', 1].data.dtype)

            # Loop through ads, pasting them in. Do gmos bias and gain hack
            for add in ad['SCI']:
                s_xmin, s_xmax, s_ymin, s_ymax = add.data_section().as_pytype()
                self.l.debug("Source Image extent is: %d:%d, %d:%d", s_xmin, s_xmax, s_ymin, s_ymax)
                d_xmin, d_xmax, d_ymin, d_ymax = add.detector_section().as_pytype()
                # Figure out which chip we are and add gap padding
                # All the gmos chips ever have been 2048 pixels in X.
                if d_xmin == 4096 or d_xmin == 5120:
                    pad = 2*gap
                elif d_xmin == 2048 or d_xmin == 3072:
                    pad = gap
                else:
                    pad = 0

                d_xmin = (d_xmin + pad) / int(ad.detector_x_bin()) - xmin
                d_xmax = (d_xmax + pad) / int(ad.detector_x_bin()) - xmin
                d_ymin = d_ymin / int(ad.detector_y_bin()) - ymin
                d_ymax = d_ymax / int(ad.detector_y_bin()) - ymin
                o_xmin, o_xmax, o_ymin, o_ymax = add.overscan_section().as_pytype()
                bias = numpy.median(add.data[o_ymin:o_ymax, o_xmin:o_xmax])
                try:
                    # This throws an exception sometimes if some of the values are None?
                    gain = float(add.gain())
                except:
                    gain = 1.0
                self.l.debug("Pasting: %d:%d,%d:%d -> %d:%d,%d:%d", s_xmin, s_xmax, s_ymin, s_ymax, d_xmin, d_xmax, d_ymin, d_ymax)
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
                xoffset = 0 if x1 < 2000 else gap
                yoffset = 0 if y1 < 2000 else gap
                self.l.debug("x1 x2 y1 y2: %d %d %d %d", x1, x2, y1, y2)
                self.l.debug("xoffset yoffset", xoffset, yoffset)
                self.l.debug("full shape: %s", full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset].shape)
                self.l.debug("data shape: %s", add.data.shape)
                full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset] = add.data

            full = norm(full)

        elif str(ad.instrument()) in {'TReCS', 'michelle'}:
            chopping = False
            full_shape = (500, 660)
            full = numpy.zeros(full_shape, numpy.float32)
            stack_shape = (240, 320)
            stack = numpy.zeros(stack_shape, numpy.float32)
            # For michelle, look up the nod-chop cycle
            cycle = ad.phu_get_key_value('CYCLE')
            if cycle is None:
                cycle = 'ABBA'
            # Loop through the extensions and stack them according to nod position
            for add in ad['SCI']:
                # Just sum up along the 4th axis. Ahem, this is the 0th axis in numpy land
                data = add.data
                data = numpy.sum(data, axis=0)
                # Now the new 0th axis is the chop position
                # If it's two long, subtract the two, otherwise just go with first plane
                chop_a = None
                chop_b = None
                if data.shape[0] == 2:
                    # Trecs
                    chopping = True
                    chop_a = data[0,:,:]
                    chop_b = data[1,:,:]
                    data = chop_a - chop_b
                elif data.shape[0] == 3:
                    # Michelle
                    chopping = True
                    chop_a = data[1,:,:]
                    chop_b = data[2,:,:]
                    data = data[0,:,:]
                else:
                    data = data[0,:,:]

                # For the first frame, paste the two raw chop images into the full image
                if chopping:
                    if add.extver() == 1:
                        chop_a = norm(chop_a)
                        chop_b = norm(chop_b)
                        full[260:500, 0:320] = chop_a
                        full[260:500, 340:660] = chop_b
                else:
                    # Not chopping, but still nodding?
                    if len(ad['SCI']) > 1:
                        if add.extver() == 1:
                            full[260:500, 0:320] = norm(data)
                        if add.extver() == 2:
                            full[260:500, 340:660] = norm(data)

                # Figure out if we're nod A or B
                # TReCS has this header in each extention
                if 'NOD' in add.header.keys():
                    nod = add.header['NOD']
                else:
                    i = add.extver() - 1
                    j = i % len(cycle)
                    nod = cycle[j]
                #print "Extver: %d, nod: %s" % (add.extver(), nod)
                if nod == 'A':
                    stack += data
                else:
                    stack -= data

            # Normalise the stack and paste it into the image
            stack = norm(stack)
            if not chopping and len(ad['SCI']) == 1:
                full = stack
            else:
                full[0:240, 180:500] = stack

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
        ax.imshow(full, cmap=plt.cm.gray)

        fig.savefig(outfile, format='jpg')

        plt.close()
