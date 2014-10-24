"""
This module holds the Calibration superclass
"""

class Calibration(object):
    """
    This class provides a basic Calibration Manager
    This is the superclass from which the instrument specific variants subclass
    """

    session = None
    header = None
    descriptors = None
    types = None
    applicable = []

    def __init__(self, session, header, descriptors, types):
        """
        Initialize a calibration manager for a given header object (ie data file)
        Need to pass in an sqlalchemy session that should already be open, this class will not close it
        Also pass in a header object
        """
        self.session = session
        self.header = header
        self.descriptors = descriptors
        self.types = types
        self.from_descriptors = False

        # Populate the descriptors dictionary for header
        if self.descriptors == None:
            self.from_descriptors = True
            self.descriptors = {}
            self.descriptors['header_id'] = self.header.id
            self.descriptors['observation_type'] = self.header.observation_type
            self.descriptors['observation_class'] = self.header.observation_class
            self.descriptors['spectroscopy'] = self.header.spectroscopy
            self.descriptors['object'] = self.header.object
            self.descriptors['instrument'] = self.header.instrument
            self.descriptors['central_wavelength'] = self.header.central_wavelength
            self.descriptors['program_id'] = self.header.program_id
            self.descriptors['ut_datetime'] = self.header.ut_datetime
            self.descriptors['exposure_time'] = self.header.exposure_time
            self.descriptors['observation_class'] = self.header.observation_class
            self.descriptors['detector_roi_setting'] = self.header.detector_roi_setting
            self.descriptors['reduction'] = self.header.reduction
            self.descriptors['elevation'] = self.header.elevation
            self.descriptors['cass_rotator_pa'] = self.header.cass_rotator_pa
        else:
            # The data_section comes over as a native python array, needs to be a string
            if self.descriptors['data_section']:
                self.descriptors['data_section'] = str(self.descriptors['data_section'])

    def bias(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def dark(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def flat(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def arc(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def fringe(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def ronchi_mask(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []

    def pinhole_mask(self, processed=False, howmany=None):
        """
        Null method for instruments that do not provide a method in their subclass
        """
        # Not defined for this instrument
        return []
