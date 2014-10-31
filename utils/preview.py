from astrodata import AstroData
import numpy
import matplotlib.pyplot as plt

def preview(ad, outfile):
    """
    Pass in an astrodata object and a file-like outfile.
    This function will create a jpeg rendering of the ad object
    and write it to the outfile
    """

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
        if 'GMOS' in str(ad.instrument()):
            o_xmin, o_xmax, o_ymin, o_ymax = add.overscan_section().as_pytype()
            bias = numpy.median(add.data[o_ymin:o_ymax, o_xmin:o_xmax])
        else:
            bias = 0
        full[d_ymin:d_ymax, d_xmin:d_xmax] = add.data[s_ymin:s_ymax, s_xmin:s_xmax] - bias
    
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
