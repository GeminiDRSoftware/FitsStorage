"""
This is the IGRINS-2 calibration class
"""

from fits_storage.core.orm.header import Header
from .calibration import Calibration


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

        filters = []
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

        # We can't use '.arc' here because they're not actually arcs, at least
        # until we have a read astrodata class for IG-2 that recognizes them
        # as arcs...
        query = self.get_query()\
            .add_filters(*filters) \
            .match_descriptors(Header.instrument)

        return query.all(howmany)
