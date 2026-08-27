"""
This is the IGRINS-2 calibration class
"""

from fits_storage.core.orm.header import Header
from .calibration import Calibration

from sqlalchemy import or_

class CalibrationIgrins2(Calibration):

    # IGRINS-2 does not have an instrument details ORM class, all the info
    # necessary for calibration association is in Header.
    instrClass = None
    instrDescriptors = ()

    def set_applicable(self):
        self.applicable = []

        if self.descriptors['observation_type'] == 'BPM':
            return

        # Presumably most files would require a BPM, but those don't exist
        # yet as of 20240805

        # OBJECT files require a FLAT and an ARC. This applies (I think) to
        # both science OBJECTS, and *cal OBJECTS, which are likely actually arcs
        if self.descriptors['observation_type'] == 'OBJECT':
            self.applicable.append('flat')
            self.applicable.append('arc')

    def bpm(self, processed=False, howmany=None):
        # In anticipation...

        # Default 1 bpm
        howmany = howmany if howmany else 1

        filters = [Header.ut_datetime <= self.descriptors['ut_datetime'], ]
        query = self.get_query() \
            .bpm(processed) \
            .add_filters(*filters) \
            .match_descriptors(Header.instrument)

        return query.all(howmany)
    
    def flat(self, processed=False, howmany=None):
        # Default 30 flats, closest in time. Note this will split some flat
        # observations if the science frame is mid-way between 2 groups of flats
        # Updated from 20 to 30 by request from Hyewon 20250123
        howmany = howmany if howmany else 30

        # FLAT exposures with less than 3.5 secs (typically 3.2) are test frames
        # that should be excluded. (HS 2025204, 20250321 doubled to 3.2)
        filters = [Header.exposure_time >= 3.5]

        # Need to add detector name once the descriptor returns something
        # consistent for both bundles and split files, then this should be good
        # for both archive (bundles) and dragons (split files)

        query = self.get_query() \
            .flat(processed) \
            .add_filters(*filters) \
            .match_descriptors(Header.instrument)

        return query.all(howmany)

    def arc(self, processed=False, howmany=None):
        # Default 1 "arc"
        howmany = howmany if howmany else 1

        filters = [Header.object == 'Blank sky',
                   Header.observation_type == 'OBJECT',
                   # Header.observation_class == 'partnerCal',
                   # These were supposed to be partnerCal but in practice they
                   # are getting taken as science.
                   ]

        # Need to add detector name once the descriptor returns something
        # consistent for both bundles and split files, then this should be good
        # for both archive (bundles) and dragons (split files)

        # We can't use '.arc' here because they're not actually arcs, at least
        # until we have a read astrodata class for IG-2 that recognizes them
        # as arcs, or we sort out non-arc wavecal functionality.
        query = self.get_query()\
            .add_filters(*filters) \
            .match_descriptors(Header.instrument)

        return query.all(howmany)

    def telluric(self, processed=False, howmany=None):
        """
        Find the optimal IGRINS-2 telluric observations for this target frame

        This will find IGRINS-2 telluric standards with matching wavelength.
        For raw data, it looks only for a qa_state of 'Pass' or 'Undefined'.
        For processed data, the 'TELLURIC' tag must be present.
        It matches within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw telluric standards
        howmany : int, default 1 if processed else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 4

        query = self.get_query().spectroscopy(True).OBJECT()
        query = query.tolerance(central_wavelength=0.001)
        query = query.match_descriptors(Header.spectroscopy)

        if processed:
            query = query.filter(Header.types.contains('TELLURIC'))
        else:
            query = query.raw().filter(
                or_(Header.observation_class == 'partnerCal',
                    Header.observation_class == 'progCal'))

            # Usable is not OK for these - may be partly saturated for example
            query = query.add_filters(
                or_(Header.qa_state == 'Pass', Header.qa_state == 'Undefined'))

        # Absolute time separation must be within 1 day
        query = query.max_interval(days=1)

        return query.all(howmany)

    def standard(self, processed=False, howmany=None):
        """
        Find the optimal IGRINS-2 (spectro)photometric standard observations for
        this target frame

        This will find IGRINS-2 flux standards with matching wavelength.
        For raw data, it looks only for a qa_state of 'Pass' or 'Undefined'.
        For processed data, the 'STANDARD' tag must be present.
        It matches within 1 day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw (spectro)
             photometric standards
        howmany : int, default 1 if processed else 8
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 4

        query = self.get_query().spectroscopy(True).OBJECT()
        # Must match: disperser, central_wavelength, focal_plane_mask, camera, filter_name
        query = query.tolerance(central_wavelength=0.001)
        query = query.match_descriptors(Header.spectroscopy)
        # AstroDataIgrins defines "STANDARD" for raw frames
        query = query.filter(Header.types.contains('STANDARD'))

        if not processed:
           # Usable is not OK for these - may be partly saturated for example
            query = query.add_filters(
                or_(Header.qa_state == 'Pass', Header.qa_state == 'Undefined'))

        # Absolute time separation must be within 1 day
        query = query.max_interval(days=1)

        return query.all(howmany)
