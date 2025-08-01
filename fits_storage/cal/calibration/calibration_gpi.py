"""
This module holds the CalibrationGPI class
"""
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.gpi import Gpi
from .calibration import Calibration

from sqlalchemy.orm import join

class CalibrationGPI(Calibration):
    """
    This class implements a calibration manager for GPI.
    It is a subclass of Calibration
    """
    instrClass = Gpi
    instrDescriptors = (
        'disperser',
        'focal_plane_mask',
        'filter_name',
        )

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GPI frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        if self.descriptors['observation_type'] == 'BPM':
            return

        # Science OBJECTs require: dark, telluric, astrometric_standard
        if ((self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):
            self.applicable.append('dark')
            self.applicable.append('astrometric_standard')
            # If spectroscopy require arc and telluric
            # Otherwise polarimetry requres polarization_flat and polarization_standard
            if self.descriptors['spectroscopy'] == True:
                self.applicable.append('arc')
                self.applicable.append('telluric')
            else:
                self.applicable.append('polarization_standard')
                self.applicable.append('polarization_flat')

        self.applicable.append('processed_bpm')

    @staticmethod
    def common_descriptors():
        # Must Totally Match: disperser, filter_name
        # Apparently FPM doesn't have to match...
        return (Gpi.disperser, Gpi.filter_name)

    def bpm(self, processed=False, howmany=None):
        """
        This method identifies the best BPM to use for the target
        dataset.

        This will match on bpms for the same instrument

        Parameters
        ----------

        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default 1 bpm
        howmany = howmany if howmany else 1

        filters = [Header.ut_datetime <= self.descriptors['ut_datetime'],]
        query = self.get_query() \
                    .bpm(processed) \
                    .add_filters(*filters) \
                    .match_descriptors(Header.instrument, Header.detector_binning)

        return query.all(howmany)

    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GPI DARK for this target frame

        This will match on darks with an exposure time within 10 seconds and taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        #  default to 1 dark for now
        howmany = howmany if howmany else 1

        query = (
            self.get_query()
                .dark(processed)
                # exposure time must be within 10 seconds difference (Paul just made that up)
                .tolerance(exposure_time=10.0)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        return query.all(howmany)

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GPI ARC for this target frame

        This will match on disperser and filter name, taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Always default to 1 arc
        howmany = howmany if howmany else 1

        query = (
            self.get_query()
                .arc(processed)
                .match_descriptors(*CalibrationGPI.common_descriptors())
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        return query.all(howmany)

    def telluric(self, processed=False, howmany=None):
        """
        Find the optimal GPI telluric standard for this target frame

        This will match on disperser and filter name, taken within 1 year.  For processed, it matches against
        calibration programs only.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw telluric standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8
        filters = []
        if not processed:
            filters = [Header.calibration_program == True]

        query = (
            self.get_query()
                .telluric(OBJECT=True, science=True)
                .add_filters(*filters)
                .match_descriptors(*CalibrationGPI.common_descriptors())
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
            )
        return query.all(howmany)

    def polarization_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization standard for this target frame

        It matches on non-spectroscopy science with a calibration program and where the GPI wollaston is set.
        For processed data, it matches a reduction state of 'PROCESSED_POLSTANDARD' instead.  Then, in either case,
        it matches data on disperser and filter name taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw polarization standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: polarization standards are only found in GPI. We won't bother moving this to CalQuery - yet
        if processed:
            # TODO I can't find this method?!
            query = self.get_query().PROCESSED_POLSTANDARD()
        else:
            query = (self.get_query().raw().science().spectroscopy(False)
                                     .add_filters(Header.calibration_program==True,
                                                  Gpi.wollaston == True))

        query = (
            query.match_descriptors(*CalibrationGPI.common_descriptors())
                 .max_interval(days=365)
            )
        return query.all(howmany)

    def astrometric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI astrometric standard field for this target frame

        This will match any data with the GPI astrometric standard flag set.  For processed
        data, it instead looks for the reduction state of 'PROCESSED_ASTROMETRIC'.  Then,
        in either case, it looks for data taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw astrometric standards.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: astrometric standards are only found in GPI. We won't bother moving this to CalQuery - yet

        if processed:
            # TODO where does this live?
            query = self.get_query().PROCESSED_ASTROMETRIC()
        else:
            query = (self.get_query().raw().OBJECT()
                         .add_filters(Gpi.astrometric_standard==True))

        # Looks like we don't care about matching the usual descriptors...
        # Absolute time separation must be within 1 year
        query =query.max_interval(days=365)
        return query.all(howmany)

    def polarization_flat(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization flat for this target frame

        This will match partnerCal datawith the GPI wollaston flag set.  For
        processed data, it looks for a reduction state of 'PROCESSED_POLFLAT' instead.
        Then, in either case, it matches on disperser and filter name  and taken within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw polarization flats.
        howmany : int, default 1 if processed, else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 8

        # NOTE: polarization flats are only found in GPI. We won't bother moving this to CalQuery - yet

        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            # TODO where does this live? document behavior above..
            query = self.get_query().PROCESSED_POLFLAT()
        else:
            query = (self.get_query().flat().partnerCal()
                         .add_filters(Gpi.wollaston == True))

        query = (
            query.match_descriptors(*CalibrationGPI.common_descriptors())
                 # Absolute time separation must be within 1 year
                 .max_interval(days=365)
            )
        return query.all(howmany)
