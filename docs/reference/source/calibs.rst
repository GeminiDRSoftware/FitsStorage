Calibration Association
=======================

Decorators
----------

A common theme across calibration association methods is that some of them cannot associate
certain type of data (processed, spectroscopy vs. imaging, etc) and will just return an
empty list for that kind. This invariably leads to find boilerplate coded like this at the
beginning of the methods:
::

    if certain_condition:
        return []

As this is a common pattern, we have implemented a number of decorators to do this job.

**NB**: The signatures shown for this decorators suggest the they accept arguments. This is
**not** the case. The signature shown is actually the one for the function wrapper that they
return, and is designed to pass arguments in a transparent way to the decorated function.

See the example code below to learn how to use them.

.. autofunction:: fits_storage.cal.calibration.not_processed
.. autofunction:: fits_storage.cal.calibration.not_imaging
.. autofunction:: fits_storage.cal.calibration.not_spectroscopy

Example code
............

We can see the decorators in action in this piece of code taken from `calibrations_gmos.py`.

.. code-block:: python
    :emphasize-lines: 1-2

    @not_processed
    @not_imaging
    def spectwilight(self, processed=False, howmany=None):
        """
        Method to find the best spectwilight - ie spectroscopy twilight
        ie MOS / IFU / LS twilight
        """

Classes
-------

.. autoclass:: fits_storage.cal.calibration.CalQuery
   :members:
   :special-members: __getattr__
