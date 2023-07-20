"""
This module holds the CalibrationNICI class
"""
import datetime

from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.nici import Nici
from .calibration import Calibration, not_processed

class CalibrationNICI(Calibration):
    """
    This class implements a calibration manager for NICI.
    It is a subclass of Calibration
    """
    instrClass = Nici
    instrDescriptors = (
        'filter_name',
        'focal_plane_mask',
        'disperser'
        )

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NICI dataset
        self.applicable = []

        if self.descriptors['observation_type'] == 'BPM':
            return

        # Science OBJECTs require a DARK and FLAT
        if (self.descriptors['observation_type'] == 'OBJECT' and self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')
            self.applicable.append('flat')

        # Lamp-on Flat fields require a lampoff_flat
        if (self.descriptors['observation_type'] == 'FLAT' and
                self.descriptors['gcal_lamp'] != 'Off'):
            self.applicable.append('lampoff_flat')

        self.applicable.append('processed_bpm')

    def bpm(self, processed=False, howmany=None, return_query=False):
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

        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def dark(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NICI Dark for this target frame

        This will find NICI darks with an exposure tie within 0.01 seconds taken within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks.
        howmany : int, default 1 of processed, else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 10

        query = (
            self.get_query()
                .dark(processed)
                # Exposure time must match to within 0.01 (nb floating point match).
                # nb exposure_time is really exposure_time * coadds, but if we're matching both, that doesn't matter
                .tolerance(exposure_time = 0.01)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    def flat(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NICI Flat for this target frame

        This will find NICI flats with a gcal_lamp of 'IRhigh' and a matching filter name, focal plane mask,
        and disperser taken within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats.
        howmany : int, default 1 if processed, else 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 10

        query = (
            self.get_query()
                .flat(processed)
                # GCAL lamp should be on - these flats will then require lamp-off flats to calibrate them
                .add_filters(Header.gcal_lamp == 'IRhigh')
                .match_descriptors(Nici.filter_name,
                                   Nici.focal_plane_mask,
                                   Nici.disperser)
                # Absolute time separation must be within 1 day
                .max_interval(days=1)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)

    @not_processed
    def lampoff_flat(self, processed=False, howmany=None, return_query=False):
        """
        Find the optimal NICI Lamp-off Flat for this target frame

        This will find NICI lamp-off flats with a gcal_lamp of 'Off' and a matching filter name, focal plane mask,
        and disperser taken within 1 hour.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats.
        howmany : int, default 10
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 10

        query = (
            self.get_query()
                .flat()
                .add_filters(Header.gcal_lamp == 'Off')
                # NOTE: check this comment...
                # Must totally match: data_section, well_depth_setting, filter_name, camera
                # Update from AS 20130320 - read mode should not be required to match, but well depth should.
                .match_descriptors(Nici.filter_name,
                                   Nici.focal_plane_mask,
                                   Nici.disperser)
                # Absolute time separation must be within 1 hour of the lamp on flats
                .max_interval(seconds=3600)
            )
        if return_query:
            return query.all(howmany), query
        else:
            return query.all(howmany)
