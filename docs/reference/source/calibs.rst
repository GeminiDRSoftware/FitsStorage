Calibration Association
=======================

The Calibration Base Class
--------------------------

.. autoclass:: fits_storage.cal.calibration.Calibration
   :members:
   :exclude-members: get_query, set_applicable, session, header, descriptors, types, applicable, instrClass, instrDescriptors
   :undoc-members:

   .. automethod:: fits_storage.cal.calibration.Calibration.set_applicable
   .. automethod:: fits_storage.cal.calibration.Calibration.get_query

   The following are the available calibration association methods. Their base implementation
   returns an empty list. Derived classes need to implement only the methods that are relevant
   to the specific instrument (eg. ``Calibration_F2`` implements ``arc``, ``dark``, ``flat``,
   etc., but not ``fringe``, ``mask``, ...)

   All methods must accept a ``processed`` flag, and a ``howmany`` argument. The ``processed``
   flag (default ``False``) can be used to return early, if an instrument doesn't provide
   processed versions of certain calibrations (see :ref:`calib-decorators`). ``howmany`` sets an
   upper limit on the number of calibrations to be returned, with the default being ``None``,
   meaning "no specific limit". The implemention of the methods in derived classes may set
   a different default limit for ``howmany``.

   .. note:: As the relevant methods are selected by *name*, there's nothing against a derived
      class defining calibration methods that are not in the following list. Those methods will
      be invoked only when the user asks for *all* available calibrations for a certain frame,
      though (the standard ones can be picked individually).

.. _calib-decorators:

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

.. note:: The signatures shown for this decorators suggest the they accept arguments. This is
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

ORM Instrument Classes
----------------------

The instrument-specific classes are meant to store attributes that are not members
in the ``orm.Header`` (which is common to all instruments). These
specific classes are used by the calibration association service, and we should not
need to create new ones if they are not required when finding calibrations.

The common pattern when creating such classes is the following:

.. code-block:: python

    class NewInstrument(Base):
        "This is the ORM object for the NewInstrument details"
        __tablename__ = "newinstrument"

        id = Column(Integer, primary_key=True)
        header_id = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
        header = relation(Header, order_by=id)
        read_mode = Column(Text, index=True)    # Instrument specific
        disperser = Column(Text, index=True)    # Instrument specific

        def __init__(self, header, ad):
            self.header = header
            self.populate(ad)

        def populate(self, ad):
            self.read_mode = ad.read_mode().for_db()
            self.disperser = ad.disperser().for_db()

The ``NewInstrument.populate`` method may process the data as we see fit. It is used only
when creating a new object, of course, before committing the data to the database. No other
methods are needed.

The Calibration Query Helper Class
----------------------------------

.. autoclass:: fits_storage.cal.calibration.CalQuery
   :members:
   :special-members: __getattr__
