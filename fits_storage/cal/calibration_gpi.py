"""
This module holds the CalibrationGPI class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gpi import Gpi
from .calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationGPI(Calibration):
    """
    This class implements a calibration manager for GPI.
    It is a subclass of Calibration
    """
    gpi = None

    def __init__(self, session, header, descriptors, types):
        """
        This is the GPI calibration object subclass init method
        """
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, find the gpiheader
        if header:
            query = session.query(Gpi).filter(Gpi.header_id == self.descriptors['header_id'])
            self.gpi = query.first()

        # Populate the descriptors dictionary for GPI
        if self.from_descriptors:
            self.descriptors['coadds'] = self.gpi.coadds
            self.descriptors['disperser'] = self.gpi.disperser
            self.descriptors['focal_plane_mask'] = self.gpi.focal_plane_mask
            self.descriptors['filter_name'] = self.gpi.filter_name

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GPI frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        # Science OBJECTs require: dark, telluric_standard, astrometric_standard
        if ((self.descriptors['observation_type'] == 'OBJECT') and 
                (self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):
            self.applicable.append('dark')
            self.applicable.append('astrometric_standard')
            # If spectroscopy require arc and telluric_standard
            # Otherwise polarimetry requres polarization_flat and polarization_standard
            if self.descriptors['spectroscopy'] == True:
                self.applicable.append('arc')
                self.applicable.append('telluric_standard')
            else:
                self.applicable.append('polarization_standard')
                self.applicable.append('polarization_flat')

    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GPI DARK for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
        else:
            query = query.filter(Header.observation_type == 'DARK').filter(Header.reduction == 'RAW')

        #  default to 1 dark for now
        howmany = howmany if howmany else 1

        # exposure time must be within 10 seconds difference (I just made that up)
        exptime_hi = float(self.descriptors['exposure_time']) + 10.0
        exptime_lo = float(self.descriptors['exposure_time']) - 10.0
        query = query.filter(Header.exposure_time > exptime_lo).filter(Header.exposure_time < exptime_hi)

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GPI ARC for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_ARC')
        else:
            query = query.filter(Header.observation_type == 'ARC').filter(Header.reduction == 'RAW')

        # Always default to 1 arc
        howmany = howmany if howmany else 1

        # Must Totally Match: disperser, focal_plane_mask, filter_name
        query = query.filter(Gpi.disperser == self.descriptors['disperser'])
        # Apparently FPM doesn't have to match...
        # query = query.filter(Gpi.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Gpi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()

    def telluric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI telluric standard for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            howmany = howmany if howmany else 1
            query = query.filter(Header.reduction == 'PROCESSED_TELLURIC')
        else:
            howmany = howmany if howmany else 8
            query = query.filter(Header.observation_type == 'OBJECT').filter(Header.reduction == 'RAW')
            query = query.filter(Header.calibration_program == True).filter(Header.observation_class == 'science')
            query = query.filter(Header.spectroscopy == True)

        # Must Totally Match: disperser, filter_name
        query = query.filter(Gpi.disperser == self.descriptors['disperser'])
        query = query.filter(Gpi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()

    def polarization_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization standard for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            howmany = howmany if howmany else 1
            query = query.filter(Header.reduction == 'PROCESSED_POLSTANDARD')
        else:
            howmany = howmany if howmany else 8
            query = query.filter(Header.observation_type == 'OBJECT').filter(Header.reduction == 'RAW')
            query = query.filter(Header.calibration_program == True).filter(Header.observation_class == 'science')
            query = query.filter(Header.spectroscopy == False).filter(Gpi.wollaston == True)

        # Must Totally Match: disperser, filter_name
        query = query.filter(Gpi.disperser == self.descriptors['disperser'])
        query = query.filter(Gpi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()


    def astrometric_standard(self, processed=False, howmany=None):
        """
        Find the optimal GPI astrometric standard field for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            howmany = howmany if howmany else 1
            query = query.filter(Header.reduction == 'PROCESSED_ASTROMETRIC')
        else:
            howmany = howmany if howmany else 8
            query = query.filter(Header.observation_type == 'OBJECT').filter(Header.reduction == 'RAW')
            query = query.filter(Gpi.astrometric_standard == True)

        # No, don't care I think - Must Totally Match: disperser, filter_name
        #query = query.filter(Gpi.disperser == self.descriptors['disperser'])
        #query = query.filter(Gpi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()


    def polarization_flat(self, processed=False, howmany=None):
        """
        Find the optimal GPI polarization flat for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gpi, Header), DiskFile))

        if processed:
            howmany = howmany if howmany else 1
            query = query.filter(Header.reduction == 'PROCESSED_POLFLAT')
        else:
            howmany = howmany if howmany else 8
            query = query.filter(Header.observation_type == 'FLAT').filter(Header.reduction == 'RAW')
            query = query.filter(Gpi.wollaston == True).filter(Header.observation_class == 'partnerCal')

        # Must Totally Match: disperser, filter_name
        query = query.filter(Gpi.disperser == self.descriptors['disperser'])
        query = query.filter(Gpi.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 year
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=365), limit=howmany)

        return query.all()


