"""
This module holds the CalibrationGNIRS class
"""
import datetime

from orm.diskfile import DiskFile
from orm.header import Header
from orm.gnirs import Gnirs
from cal.calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationGNIRS(Calibration):
    """
    This class implements a calibration manager for GNIRS.
    It is a subclass of Calibration
    """
    gnirs = None

    def __init__(self, session, header, descriptors, types):
        """
        This is the GNIRS calibration object subclass init method
        """
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # Find the gnirsheader
        query = session.query(Gnirs).filter(Gnirs.header_id == self.descriptors['header_id'])
        self.gnirs = query.first()

        # Populate the descriptors dictionary for GNIRS
        if self.from_descriptors:
            self.descriptors['read_mode'] = self.gnirs.read_mode
            self.descriptors['well_depth_setting'] = self.gnirs.well_depth_setting
            self.descriptors['coadds'] = self.gnirs.coadds
            self.descriptors['disperser'] = self.gnirs.disperser
            self.descriptors['focal_plane_mask'] = self.gnirs.focal_plane_mask
            self.descriptors['camera'] = self.gnirs.camera
            self.descriptors['filter_name'] = self.gnirs.filter_name

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        """
        This method determines the list of applicable calibration types
        for this GNIRS frame and writes the list into the class
        applicable variable.
        It is called from the subclass init method.
        """
        self.applicable = []

        # Science Imaging OBJECTs that are not acq or acqCal require a DARK
        if ((self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal']) and
                (self.descriptors['spectroscopy'] == False)):
            self.applicable.append('dark')

        # Spectroscopy OBJECT frames require a flat and arc
        if (self.descriptors['observation_type'] == 'OBJECT') and (self.descriptors['spectroscopy'] == True):
            self.applicable.append('flat')
            self.applicable.append('arc')
            self.applicable.append('pinhole_mask')


    def dark(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS Dark for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gnirs, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
            # Default number of processed darks to associate
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.observation_type == 'DARK').filter(Header.reduction == 'RAW')
            # Default number of raw darks to associate
            howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, well_depth_setting, exposure_time, coadds
        query = query.filter(Gnirs.read_mode == self.descriptors['read_mode'])
        query = query.filter(Gnirs.well_depth_setting == self.descriptors['well_depth_setting'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])
        query = query.filter(Gnirs.coadds == self.descriptors['coadds'])

        # Absolute time separation must be within 3 months
        max_interval = datetime.timedelta(days=90)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

    def flat(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS flat field for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gnirs, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number of processed flats to associate
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.observation_type == 'FLAT').filter(Header.reduction == 'RAW')
            # Default number of raw flats to associate
            howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength, focal_plane_mask, camera, filter_name, well_depth_setting
        # update from RM 20130321 - read mode should not be required to match, but well depth should.
        query = query.filter(Gnirs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Gnirs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Gnirs.camera == self.descriptors['camera'])
        query = query.filter(Gnirs.filter_name == self.descriptors['filter_name'])
        query = query.filter(Gnirs.well_depth_setting == self.descriptors['well_depth_setting'])

        # Absolute time separation must be within 3 months
        max_interval = datetime.timedelta(days=90)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

    def arc(self, processed=False, howmany=None):
        """
        Find the optimal GNIRS ARC for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gnirs, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_ARC')
        else:
            query = query.filter(Header.observation_type == 'ARC').filter(Header.reduction == 'RAW')

        # Always default to 1 arc
        howmany = howmany if howmany else 1

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, camera
        query = query.filter(Gnirs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Gnirs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Gnirs.filter_name == self.descriptors['filter_name'])
        query = query.filter(Gnirs.camera == self.descriptors['camera'])

        # Absolute time separation must be within 1 year
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

    def pinhole_mask(self, processed=False, many=None):
        """
        Find the optimal GNIRS pinhole_mask for this target frame
        """
        query = self.session.query(Header).select_from(join(join(Gnirs, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_PINHOLE')
            # Default number of processed pinholes
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.observation_type == 'PINHOLE').filter(Header.reduction == 'RAW')
            # Default number of raw pinholes
            howmany = howmany if howmany else 2

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength, camera, (only for cross dispersed mode?)
        query = query.filter(Gnirs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Gnirs.camera == self.descriptors['camera'])

        # Absolute time separation must be within 1 year
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()
