"""
This module contains a helper class for generating preview images
"""
import os
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from sqlalchemy.exc import NoResultFound

from gemini_instruments.gmos.pixel_functions import get_bias_level
from gempy.library.spectral import Spek1D

from fits_storage.logger import DummyLogger
from fits_storage.config import get_config

from fits_storage.server.orm.preview import Preview


class PreviewException(Exception):
    pass


class Previewer(object):
    """
    This class contains all the code to generate preview images. We instantiate
    it with a diskfile object, database session, and optional logger and
    configuration parameters, then call methods on it to generate the preview.
    """
    diskfile = None
    session = None
    logger = None
    path = None
    using_s3 = None
    filename = None
    fpfn = None  # Full Path Filename
    spectrum = None

    def __init__(self, diskfile, session, logger=None, path = None,
                 using_s3 = None, force=False, scavengeonly=False):
        self.diskfile = diskfile
        self.session = session
        self.logger = logger if logger is not None else DummyLogger()

        fsc = get_config()
        self.using_s3 = using_s3 if using_s3 is not None else fsc.using_s3
        self.path = path if path is not None else fsc.previewpath
        self.path = fsc.s3_staging_dir if self.using_s3 else self.path

        self.filename = diskfile.filename.removesuffix('.bz2')\
            .replace('.fits', '.jpg')

        self.fpfn = os.path.join(self.path, self.filename)

        self.spectrum = len(diskfile.get_ad_object[0].shape) == 1

        self.force = force
        self.scavengeonly = scavengeonly

        if self.using_s3:
            from fits_storage.server.aws_s3 import Boto3Helper
            self.s3 = Boto3Helper()
    def delete_file(self):
        """
        Delete the preview file. Fail silently if unable
        """
        try:
            os.unlink(self.fpfn)
        except Exception:
            pass


    def make_preview(self):
        """
        This is the general "do it all" call once you're instantiated the
        preview object.

        If force is true, it will always re-create the preview file, and
        will ensure there is a database entry for it.

        If Force is false, it will not re-create an existing preview file that
        has an appropriate filename for the diskfile in question. It will create
        a preview ORM entry if it does not exist. nb - this
        subsumes the previous "scavenge" functionality transparently.

        If scavengeonly is True it will not create new preview files that do
        not exist, but will create database entries where the file exists but
        the database entry does not.
        """

        # Does a preview file already exist for this preview?
        exists = False
        if self.using_s3:
            exists = self.s3.exists_key(self.filename)
            self.logger.debug("S3 key exists: %s", exists)
        else:
            if os.path.exists(self.fpfn):
                self.logger.debug("Preview file already exists: %s", self.fpfn)
                exists = True

        # Decide whether to (re-) create the preview file...
        if self.force or (not exists and not self.scavengeonly):
            status = self.make_preview_file()
        else:
            # Not force, file already exists, or file doesn't exist but only
            # scavenging
            status = True

        # If the preview file status is good, ensure that a database entry
        # exists for it.

        # Find the database entry if it exists...
        try:
            dbp = self.session.query(Preview) \
                .filter(Preview.diskfile_id == self.diskfile.id).one()
        except NoResultFound:
            dbp = None

        if status:
            if dbp:
                self.logger.debug("DB preview already exists for this diskfile")
                if dbp.filename != self.filename:
                    self.logger.debug("correcting the filename, though...")
                    dbp.filename = self.filename
            else:
                self.logger.debug("Creating DB preview entry")
                dbp = Preview(self.diskfile, self.filename)
                self.session.add(dbp)
        else:
            # If the status is bad, delete any preview database entry.
            if dbp is not None:
                self.logger.debug("Preview status was bad and preview exists"
                                  "in database - deleting")
                self.session.delete(dbp)
        self.session.commit()

        if self.using_s3 and status:
            # Upload preview file to S3 and delete local copy
            self.logger.error("Preview upload to S3 needs implementing")

        return status

    def make_preview_file(self):
        """
        Make the preview file if possible. Overwrites any existing file.
        Creates a jpg file at self.fpfn that contains a preview image.

        Return True on Success, False on error
        """
        if not os.path.isdir(self.path):
            self.logger.error("Previewpath %s is not a directory")
            return False

        if os.path.exists(self.fpfn):
            self.logger.warning("Removing existing preview file %s", self.fpfn)
            self.delete_file()

        try:
            with open(self.fpfn, 'wb') as fp:
                if self.spectrum:
                    rendered = self.render_spectrum(fp)
                else:
                    rendered = self.render_image(fp)
            if rendered:
                return True
            else:
                self.logger.warning("Could not render preview for %s",
                                    self.diskfile.filename)
                self.delete_file()
                return False
        except IOError:
            self.logger.error("IO Error on %s", self.fpfn)
            self.delete_file()
            return False
        except PreviewException:
            self.logger.error("Unable to make preview for %s",
                              self.diskfile.filename)
            self.delete_file()
            return False

    def render_spectrum(self, fp):
        """
        Not implemented yet
        """
        self.logger.warning("Spectrum preview not implemented yet")
        return False

    def norm(self, data, percentile=0.3):
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


    def render_image(self, fp):
        """
        This function will create a jpeg rendering of the ad object and write
        it to the filelike fp.
        """
        ad = self.diskfile.get_ad_object

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

            self.logger.debug(fmt1.format(xmin, xmax, ymin, ymax))

            # Make empty array for full image
            gap = 40  # approx chip gap in pixels
            shape = (ymax - ymin, (xmax - xmin) + 2 * gap)

            # needs to not be uint16 before bias/gain adjust (per ext)
            full = numpy.zeros(shape, numpy.float64)

            # Loop through ads, pasting them in. Do gmos bias and gain hack
            if len(ad[0].data.shape) == 1:
                # spectra
                full = ad[0].data
                full = self.norm(full)
            else:
                for add in ad:
                    s_xmin, s_xmax, s_ymin, s_ymax = add.data_section()
                    self.logger.debug(fmt2.format(s_xmin, s_xmax, s_ymin, s_ymax))
                    d_xmin, d_xmax, d_ymin, d_ymax = add.detector_section()
                    # Figure out which chip we are and add gap padding
                    # All the gmos chips ever have been 2048 pixels in X.
                    if d_xmin == 4096 or d_xmin == 5120:
                        pad = 2 * gap
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
                        bias = numpy.median(
                            add.data[o_ymin:o_ymax, o_xmin:o_xmax])
                    except:
                        try:
                            bias = get_bias_level(add, estimate=True)
                        except:
                            self.logger.warn("Unable to read overscan, "
                                             "using 0 bias for preview")
                            bias = 0
                    try:
                        # This throws an exception sometimes if some of the values are None?
                        gain = float(add.gain())
                    except:
                        gain = 1.0
                    self.logger.debug(fmt3.format(s_xmin, s_xmax, s_ymin,
                                                  s_ymax, d_xmin, d_xmax,
                                                  d_ymin, d_ymax))
                    full[d_ymin:d_ymax, d_xmin:d_xmax] = (add.data[
                                                          s_ymin:s_ymax,
                                                          s_xmin:s_xmax] - bias) * gain
                full = self.norm(full, percentile=5)

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
                self.logger.debug("x1 x2 y1 y2: {} {} {} {}".format(x1, x2, y1, y2))
                self.logger.debug("xoffset yoffset", xoffset, yoffset)
                self.logger.debug("full shape: {}".format(
                    full[y1 + yoffset:y2 + yoffset,
                    x1 + xoffset:x2 + xoffset].shape))
                self.logger.debug("data shape: {}".format(add.data.shape))
                full[y1 + yoffset:y2 + yoffset,
                x1 + xoffset:x2 + xoffset] = add.data

            full = self.norm(full)

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
                    chop_a = data[0, :, :]
                    chop_b = data[1, :, :]
                    data = chop_a - chop_b
                elif data.shape[0] == 3:
                    # Michelle
                    chopping = True
                    chop_a = data[1, :, :]
                    chop_b = data[2, :, :]
                    data = data[0, :, :]
                else:
                    data = data[0, :, :]

                # For the first frame, paste the two raw chop images into the full image
                extver = add.hdr['EXTVER']
                if chopping:
                    if extver == 1:
                        chop_a = self.norm(chop_a)
                        chop_b = self.norm(chop_b)
                        full[260:500, 0:320] = chop_a
                        full[260:500, 340:660] = chop_b
                else:
                    # Not chopping, but still nodding?
                    if len(ad) > 1:
                        if extver == 1:
                            full[260:500, 0:320] = self.norm(data)
                        if extver == 2:
                            full[260:500, 340:660] = self.norm(data)

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
            stack = self.norm(stack)
            if not chopping and len(ad) == 1:
                full = stack
            else:
                full[0:240, 180:500] = stack
        elif str(ad.instrument()) == "GHOST" and 'BUNDLE' in ad.tags:
            full = ad[1].data

            full = self.norm(full)
        else:
            # Generic plot the first extention case
            full = ad[0].data

            # Do a numpy squeeze on it - this collapses any axis with 1-pixel extent
            full = numpy.squeeze(full)

            full = self.norm(full)

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
            # mask values below a certain threshold
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
                self.logger.warning("Recovering (simplified preview) from Exception", exc_info=True)
                plt.plot(flux_masked)
                plt.plot(variance_masked, color='r')
        else:
            ax = plt.Axes(fig, [0, 0, 1, 1])
            ax.set_axis_off()
            fig.add_axes(ax)
            ax.imshow(full, cmap=plt.cm.gray)

        fig.savefig(fp, format='jpg')

        plt.close()
        return True


    # This isn't used - needs some refactoring and rework to paste all the
    # spectra into one plot and do away with the idx thing.
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
        # mask values below a certain threshold
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
            self.logger.debug("Exception. Generating simplified preview instead",
                         exc_info=True)
            plt.plot(flux_masked)
            plt.plot(variance_masked, color='r')

        fig.savefig(outfile, format='jpg')

        plt.close()
        return True
