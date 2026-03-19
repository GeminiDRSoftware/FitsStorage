# GHOST Instrument Wavelength Monitoring

We want to monitor the stability of GHOST wavelength calibration, by looking at
the results of arc reduction. GHOST has some complexity here.

GHOST `attachWavelengthSolution` uses the .WFIT extension from the processed arc
which encapsulates the result of the arc line fitting. This takes the form of a
6x6 image (ie grid of values). At this point, a spectrum from each order has
been extracted from the 2D image and the data array consists of an eg 33x6144 
pixel image with one line per order - 33 orders x 6144 pixels.

`attachWavelengthSolution` uses the `PolySpect` class from 
`DRAGONS/geminidr/ghost/polyfit /polyspect.py` to expand the 6x6 .WFIT into an
Norders x Npixels (eg 33x6144) grid of wavelength values in a non-obvious 
"polynomial of polynomials" way as follows:

To avoid confusion in what follows, we refer to polynomials as nth-degree (ie
a quadratic would be a 2nd degree polynomial). We reserve the term order for
the diffraction orders.

The 6x6 grid of parameters represents the coefficients of 6 6th-degree 
polynomials. Each 6th-degree polynomial is used to calculate the coeffients
of another 6th-degreee polynomial tha maps pixel value onto wavelength for a 
given order.

Backing up a bit - a few things to note in `polyspect.py`:

y_value refers to a (possibly rescaled) *x* pixel coordinate for GHOST. 
PolySpect is a generic class and when instantiated for GHOST, 
`self.transpose = True`. References to "y" refer to the dispersion direction.

m refers to order number. m_min, m_max are what you'd expect. m_ref
is a "reference" order, which will become significant later. `orders` is a list
of orders we're considering. `mprime` is a rescaled order number, or actually
a list (equivalent to `orders`) of the rescaled order numbers. `mprime` is 
0.0 for the reference order.

OK, the guts of the polynomial of polynomials expansion:

As a reminder, we are expanding the 6x6 .WFIT ad/FITS extension (which is passed
into `params` in this code) into a NordersxNpixels grid of wavelength values.

At `polyspect.py:178` we initialze a `polynomials` array, of size 33x6 
(ie Norders x N_polynomial_degrees). This will contain the coefficients of the
polynomials that map (rescaled) pixel coordinate to wavelength for each order.

Each (say the i-th) column in the 6x6 grid gives the coefficients of a 
6th-degree polynomial, which when evaluated on `mprime` gives the i-th 
coefficient of the polynomial that maps (rescaled) pixel value to wavelength for
the order corresponding to `mprime` (`mprime` is just a rescaled order number)

At `polyspect.py:180` we loop through the polynomial degrees, instantiate the 
polynomial to calculate the i-th degree coefficients (from `params[i, :]`) and
evaluate that polynomial on each `mprime` value to populate `polynomials`.

Note at this point that for the reference order (`m_ref`), the `mprime` value is
zero, and a polynomial of any degree evaluates on zero is just the zeroth-degree
coefficient. Thus, the row in the `polynomials` grid corresponding to m_ref is
equal to the column in the params (6x6) grid corresponding to 0-th degree. 
ie:

```
>>> gs.m_ref
50
>>> orders[17]
50
>>> # ie the reference order is 50 and that is the 17th order in the grid
>>> polynomials[17]
array([ 8.11941008e-19, -1.15331715e-14,  2.04472690e-10, -9.00750964e-07,
        3.65147737e-02,  6.90067021e+03])
>>> params[:, 5]
array([ 8.11941008e-19, -1.15331715e-14,  2.04472690e-10, -9.00750964e-07,
        3.65147737e-02,  6.90067021e+03], dtype='>f8')
```

As per `np.poly1d()` the coefficients are given in high-to-low degree order.

At `polyspect.py:185` we loop through orders and evaluate the polynomials in the
rows of `polynomials` to generate the final wavelengths grid, which is returned
to the caller (and becomes wfit in `attachWavelengthSolution`, which can 
interpolate between a before and after arc).

Note that a rescaled pixel coordinate value (`y_values[i] - self.szy // 2`) is
used, so there will be corresponding scale factors on the dispersion and higher
order terms.

For the instrument monitoring though, the crucial point is that 
`ad[0].WFIT[:, 5]` gives the polynomial for the reference order, and those 
coefficients (at least the low order ones) are meaningful to monitor.
Changes in the 0-degree term represent a wavelength shift, changes in the
1-degree term a dispersion change, etc.
