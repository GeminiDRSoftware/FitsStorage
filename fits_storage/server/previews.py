"""
This module provides various utility functions to manage and service the preview
queue.

"""
import os

import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from .orm.preview import Preview

from fits_storage.config import get_config
#fsc = get_config()

if fsc.using_s3:
    from .aws_s3 import get_helper

import astrodata
import gemini_instruments
from gemini_instruments.gmos.pixel_functions import get_bias_level
from gempy.library.spectral import Spek1D


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


def make_preview(self, diskfile, force=False):
    """
    Make the preview, given the diskfile. This can be called either from
    the preview queue servicing, or from file ingesting.

    The diskfile object handles providing an ad_object for this code to use

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            DiskFile record to make preview for
        force : bool
            If True, force (re)creation of the preview even if one already
            exists
    """
    fsc = get_config()
    # Generate the path to store the preview files to

    if fsc.using_s3:
        # Create the file in s3_staging_area
        preview_path = os.path.join(fsc.s3_staging_area)
    else:
        # Create the file in the preview_path in the storage_root
        preview_path = os.path.join(fsc.storage_root, fsc.preview_path)

    preview_num = 0
    for extnum in range(len(diskfile.ad_object)):
        try:
            preview_fullpath = os.join(preview_path, "%s_%03d_preview.jpg"
                                       % (diskfile.filename, extnum))
            if False:  # spect code needs more work.
            #if len(diskfile.ad_object[0].shape) == 1:
                # It's a spectrum
                with open(filename, 'wb') as fp:
                    try:
                        self.render_spectra_preview(diskfile.ad_object, fp, idx)
                    except:
                        os.unlink(filename)
                        raise

                # If we're not using S3, that's it, the file is in place.
                # If we are using s3, need to upload it now.
                if using_s3:
                    # Create the file in s3_staging_area
                    prv_fullpath = os.path.join(s3_staging_area, filename)
                else:
                        # Create the preview filename
                        prv_fullpath = os.path.join(storage_root, preview_path, filename)

                    if using_s3:
                        self.s3.upload_file(filename, prv_fullpath)
                        os.unlink(prv_fullpath)

                    # Add to preview table
                    p_check = self.s.query(Preview).filter(Preview.diskfile_id == diskfile.id,
                                                           Preview.filename == filename).first()
                    if p_check is None:
                        preview = Preview(diskfile, filename)
                        self.s.add(preview)
            else:
                # It's imaging.
                with open(preview_fullpath, 'wb') as fp:
                    try:
                        self.render_preview(diskfile.ad_object, fp)
                    except:
                        os.unlink(preview_fullpath)
                        raise
                # Now we should have a preview in fp. Close the file-object

                # If we're not using S3, that's it, the file is in place.
                # If we are using s3, need to upload it now.
                if using_s3:
                    self.s3.upload_file(preview_filename, preview_fullpath)
                    os.unlink(preview_fullpath)

                # Add to preview table
                # Add to preview table
                p_check = self.s.query(Preview).filter(Preview.diskfile_id == diskfile.id,
                                                       Preview.filename == preview_filename).first()
                if p_check is None:
                    preview = Preview(diskfile, preview_filename)
                    self.s.add(preview)
        finally:
            # Do any cleanup from above
            if our_dfado:
                if using_s3:
                    os.unlink(munged_fullpath)
                if our_dfcc:
                    os.unlink(ad_fullpath)

    def render_spectra_preview(self, ad, outfile, idx):
        """
        Pass in an astrodata object and a file-like outfile. This function will
        create a jpeg rendering of the ad object and write it to the outfile.

        Parameters:
        ----------
        ad: <AstroData> 
            An instance of AstroData

        outfile: <str>
           Filename to write.

        Returns:
        -------
        <void>

        """
        add = ad[idx]

        # plot without axes or frame
        fig = plt.figure(frameon=False)
    
        spek = Spek1D(add)
        flux = spek.flux
        variance = numpy.sqrt(spek.variance)
        #mask values below a certain threshold
        flux_masked = numpy.ma.masked_where(spek.mask == 16, flux)
        variance_masked = numpy.ma.masked_where(spek.mask == 16, variance)

        try:
            if len(ad) > 1:
                plt.title(spek.filename)
            else:
                plt.title("%s - %d" % (spek.filename, idx))
            plt.xlabel("wavelength %s" % spek.spectral_axis_unit)
            plt.ylabel("flux density %s" % spek.unit)
        except Exception as e:
            pass
        try:
            x_axis = spek.spectral_axis
            # full = full[~numpy.isnan(full)]
            # full = numpy.squeeze(full)
            plt.plot(x_axis, flux_masked, label="data")
            plt.plot(x_axis, variance_masked, color='r', label="stddev")
            plt.legend()
        except Exception as e:
            self.l.debug("Exception. Generating simplified preview instead",
                         exc_info=True)
            plt.plot(flux_masked)
            plt.plot(variance_masked, color='r')

        fig.savefig(outfile, format='jpg')

        plt.close()

    def render_preview(self, ad, outfile):
        """
        Pass in an astrodata object and a file-like outfile. This function will
        create a jpeg rendering of the ad object and write it to the outfile.

        Parameters:
        ----------
        ad: <AstroData> 
            An instance of AstroData

        outfile: <str>
           Filename to write.

        Returns:
        -------
        <void>

        """
        fmt1 = "Full Image extent is: {}:{}, {}:{}"
        fmt2 = "Source Image extent is: {}:{}, {}:{}"
        fmt3 = "Pasting: {}:{},{}:{} -> {}:{},{}:{}"

        if 'GMOS' in str(ad.instrument()) and len(ad) > 1:
            # Find max extent in detector pixels
            xmin = 10000
            ymin = 10000
            xmax = 0
            ymax = 0

            for sect in ad.detector_section():
                [x1, x2, y1, y2] = sect
                xmin, ymin = min(x1, xmin), min(y1, ymin)
                xmax, ymax = max(x2, xmax), max(y2, ymax)

            # Divide by binning
            xmin /= int(ad.detector_x_bin())
            ymin /= int(ad.detector_y_bin())
            xmax /= int(ad.detector_x_bin())
            ymax /= int(ad.detector_y_bin())

            xmin = int(xmin)
            xmax = int(xmax)
            ymin = int(ymin)
            ymax = int(ymax)

            self.l.debug(fmt1.format(xmin, xmax, ymin, ymax))

            # Make empty array for full image
            gap = 40 # approx chip gap in pixels
            shape = (ymax-ymin, (xmax-xmin)+2*gap)
            # full = numpy.zeros(shape, ad[0].data.dtype)
            full = numpy.zeros(shape, numpy.float64)  # needs to not be uint16 before bias/gain adjust (per ext)

            # Loop through ads, pasting them in. Do gmos bias and gain hack
            if len(ad[0].data.shape) == 1:
                # spectra
                full = ad[0].data
                full = norm(full)
            else:
                for add in ad:
                    s_xmin, s_xmax, s_ymin, s_ymax = add.data_section()
                    self.l.debug(fmt2.format(s_xmin, s_xmax, s_ymin, s_ymax))
                    d_xmin, d_xmax, d_ymin, d_ymax = add.detector_section()
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

                    d_xmin = int(d_xmin)
                    d_xmax = int(d_xmax)
                    d_ymin = int(d_ymin)
                    d_ymax = int(d_ymax)

                    try:
                        o_xmin, o_xmax, o_ymin, o_ymax = add.overscan_section()
                        bias = numpy.median(add.data[o_ymin:o_ymax, o_xmin:o_xmax])
                    except:
                        try:
                            bias = get_bias_level(add, estimate=True)
                        except:
                            self.l.warn("Unable to read overscan, using 0 bias for preview")
                            bias = 0
                    try:
                        # This throws an exception sometimes if some of the values are None?
                        gain = float(add.gain())
                    except:
                        gain = 1.0
                    self.l.debug(fmt3.format(s_xmin, s_xmax, s_ymin, s_ymax,
                                            d_xmin, d_xmax, d_ymin, d_ymax))
                    full[d_ymin:d_ymax, d_xmin:d_xmax] = (add.data[s_ymin:s_ymax, s_xmin:s_xmax] - bias) * gain
                full = norm(full, percentile=5)

        elif str(ad.instrument()) == 'GSAOI':
            gap = 125
            size = 4096 + gap
            shape = (size, size)
            full = numpy.zeros(shape, ad[0].data.dtype)
            # Loop though ads, paste them in
            for add in ad:
                x1, x2, y1, y2 = add.detector_section()
                xoffset = 0 if x1 < 2000 else gap
                yoffset = 0 if y1 < 2000 else gap
                self.l.debug("x1 x2 y1 y2: {} {} {} {}".format(x1, x2, y1, y2))
                self.l.debug("xoffset yoffset", xoffset, yoffset)
                self.l.debug("full shape: {}".format(full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset].shape))
                self.l.debug("data shape: {}".format(add.data.shape))
                full[y1+yoffset:y2+yoffset, x1+xoffset:x2+xoffset] = add.data

            full = norm(full)

        elif str(ad.instrument()) in {'TReCS', 'michelle'}:
            chopping = False
            full_shape = (500, 660)
            full = numpy.zeros(full_shape, numpy.float32)
            stack_shape = (240, 320)
            stack = numpy.zeros(stack_shape, numpy.float32)
            # For michelle, look up the nod-chop cycle
            cycle = ad.phu.get('CYCLE')
            if cycle is None:
                cycle = 'ABBA'
            # Loop through the extensions and stack them according to nod position
            for add in ad:
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
                extver = add.hdr['EXTVER']
                if chopping:
                    if extver == 1:
                        chop_a = norm(chop_a)
                        chop_b = norm(chop_b)
                        full[260:500, 0:320] = chop_a
                        full[260:500, 340:660] = chop_b
                else:
                    # Not chopping, but still nodding?
                    if len(ad) > 1:
                        if extver == 1:
                            full[260:500, 0:320] = norm(data)
                        if extver == 2:
                            full[260:500, 340:660] = norm(data)

                # Figure out if we're nod A or B
                # TReCS has this header in each extention
                if 'NOD' in list(add.header.keys()):
                    nod = add.header['NOD']
                else:
                    i = add.extver() - 1
                    j = i % len(cycle)
                    nod = cycle[j]
                if nod == 'A':
                    stack += data
                else:
                    stack -= data

            # Normalise the stack and paste it into the image
            stack = norm(stack)
            if not chopping and len(ad) == 1:
                full = stack
            else:
                full[0:240, 180:500] = stack
        elif str(ad.instrument()) == "GHOST" and 'BUNDLE' in ad.tags:
            full = ad[1].data

            full = norm(full)
        else:
            # Generic plot the first extention case
            full = ad[0].data

            # Do a numpy squeeze on it - this collapses any axis with 1-pixel extent
            full = numpy.squeeze(full)

            full = norm(full)

        if full.ndim > 1:
            # flip the image - seems we need this for everything
            full = numpy.flip(full, 0)

        # plot without axes or frame
        fig = plt.figure(frameon=False)
        
        if full.ndim == 1:
            # plot a spectra
            # full = ad[0].data
            # full = full[~numpy.isnan(full)]
            # full = numpy.squeeze(full)
            spek = Spek1D(ad[0])
            flux = spek.flux
            variance = numpy.sqrt(spek.variance)
            #mask values below a certain threshold
            flux_masked = numpy.ma.masked_where(spek.mask == 16, flux)
            variance_masked = numpy.ma.masked_where(spek.mask == 16, variance)

            try:
                plt.title(spek.filename)
                plt.xlabel("wavelength %s" % spek.spectral_axis_unit)
                plt.ylabel("flux density %s" % spek.unit)
            except Exception as e:
                pass
            try:
                x_axis = spek.spectral_axis
                # full = full[~numpy.isnan(full)]
                # full = numpy.squeeze(full)
                plt.plot(x_axis, flux_masked, label="data")
                plt.plot(x_axis, variance_masked, color='r', label="stddev")
                plt.legend()
            except Exception as e:
                #self.l.error("Recovering (simplified preview) from Exception", exc_info=True)
                plt.plot(flux_masked)
                plt.plot(variance_masked, color='r')
        else:
            ax = plt.Axes(fig, [0, 0, 1, 1])
            ax.set_axis_off()
            fig.add_axes(ax)
            ax.imshow(full, cmap=plt.cm.gray)

        fig.savefig(outfile, format='jpg')

        plt.close()

