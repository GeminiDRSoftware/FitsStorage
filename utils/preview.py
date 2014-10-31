from astrodata import AstroData
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

from orm.preview import Preview

from fits_storage_config import using_s3, storage_root, preview_path

def make_preview(session, diskfile):
    """
    Make the preview, given the diskfile.
    This is called from within service_ingest_queue ingest_file
    - it will use the pre-fetched / pre-decompressed / pre-opened astrodata object if possible
    - the diskfile object should contain an ad_object member which is an AstroData instance
    """

    # Setup the preview file
    if using_s3:
        # Create a bytesIO instance for the jpeg data
        fp = io.BytesIO()
    else:
        # Create the preview filename 
        preview_filename = diskfile.filename + "_preview.jpg"
        preview_fullpath = os.path.join(storage_root, preview_path, preview_filename)
        fp = open(preview_fullpath, 'w')

    # render the preview jpg
    render_preview(diskfile.ad_object, fp)

    # close the file / upload it to s3
    if using_s3:
        # TODO
        pass
    else:
        fp.close()

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
            full[d_ymin:d_ymax, d_xmin:d_xmax] = add.data[s_ymin:s_ymax, s_xmin:s_xmax] - bias
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
