"""
This module holds the CalibrationNIFS class
"""
import datetime

from orm.diskfile import DiskFile
from orm.header import Header
from orm.nifs import Nifs
from cal.calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationNIFS(Calibration):
    """
    This class implements a calibration manager for NIFS.
    It is a subclass of Calibration
    """
    nifs = None

    def __init__(self, session, header, descriptors, types):
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # Find the nifsheader
        query = session.query(Nifs).filter(Nifs.header_id == self.descriptors['header_id'])
        self.nifs = query.first()

        # Populate the descriptors dictionary for NIFS
        if self.from_descriptors:
            self.descriptors['read_mode'] = self.nifs.read_mode
            self.descriptors['coadds'] = self.nifs.coadds
            self.descriptors['disperser'] = self.nifs.disperser
            self.descriptors['focal_plane_mask'] = self.nifs.focal_plane_mask
            self.descriptors['filter_name'] = self.nifs.filter_name

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        # Return a list of the calibrations applicable to this NIFS dataset
        self.applicable = []

        # Science Imaging OBJECTs require a DARK
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['spectroscopy'] == False and
                self.descriptors['observation_class'] == 'science'):
            self.applicable.append('dark')

        # Science spectroscopy that is not a progcal or partnercal requires a flat, arc, ronchi_mask and telluric_standard
        if (self.descriptors['observation_type'] == 'OBJECT' and
                self.descriptors['observation_class'] not in ['partnerCal', 'progCal'] and
                self.descriptors['spectroscopy'] == True):
            self.applicable.append('flat')
            self.applicable.append('processed_flat')
            self.applicable.append('arc')
            self.applicable.append('ronchi_mask')
            self.applicable.append('telluric_standard')

        # Flats require lampoff_flats
        if self.descriptors['observation_type'] == 'FLAT' and self.descriptors['gcal_lamp'] != 'Off':
            self.applicable.append('lampoff_flat')


    def dark(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
            # Default number of processed darks to associate
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Default number of processed darks to associate
            howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: read_mode, exposure_time, coadds, disperser
        query = query.filter(Nifs.read_mode == self.descriptors['read_mode'])
        query = query.filter(Header.exposure_time == self.descriptors['exposure_time'])
        query = query.filter(Nifs.coadds == self.descriptors['coadds'])
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])

        # Absolute time separation must be within ~3 months
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
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number of processed flats to associate
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Default number of processed flats to associate
            howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength, focal_plane_mask, filter
        # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Nifs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nifs.filter_name == self.descriptors['filter_name'])

        # GCAL lamp must be IRhigh or QH
        query = query.filter(Header.gcal_lamp.in_(['IRhigh', 'QH']))

        # Absolute time separation must be within 10 days
        max_interval = datetime.timedelta(days=10)
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

    def lampoff_flat(self, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))
        query = query.filter(Header.observation_type == 'FLAT')

        query = query.filter(Header.reduction == 'RAW')
        # Default number of processed flats to associate
        howmany = howmany if howmany else 10

        # Search only canonical entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength, focal_plane_mask, filter
        # NIFS flats are always taken in short / high readmode. Don't match against readmode (inst sci Email 2013-03-13)
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Nifs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nifs.filter_name == self.descriptors['filter_name'])

        # GCAL lamp must be Off
        query = query.filter(Header.gcal_lamp == 'Off')

        # Absolute time separation must be within 1 hour
        max_interval = datetime.timedelta(seconds=3600)
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

    def arc(self, sameprog=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))
        query = query.filter(Header.observation_type == 'ARC')

        # Always associate 1 arc by default
        howmany = howmany if howmany else 1

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Nifs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nifs.filter_name == self.descriptors['filter_name'])

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

    def ronchi_mask(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))
        query = query.filter(Header.observation_type == 'RONCHI')

        # Always associate 1 rocnhi by default
        howmany = howmany if howmany else 1

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match: disperser, central_wavelength
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])

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

    def telluric_standard(self, processed=False, howmany=None):
        query = self.session.query(Header).select_from(join(join(Nifs, Header), DiskFile))

        if processed:
            howmany = 1
            query = query.filter(Header.reduction == 'PROCESSED_TELLURIC')
        else:
            query = query.filter(Header.observation_type == 'OBJECT').filter(Header.observation_class == 'partnerCal')
            howmany = 12


        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter
        query = query.filter(Nifs.disperser == self.descriptors['disperser'])
        query = query.filter(Header.central_wavelength == self.descriptors['central_wavelength'])
        query = query.filter(Nifs.focal_plane_mask == self.descriptors['focal_plane_mask'])
        query = query.filter(Nifs.filter_name == self.descriptors['filter_name'])

        # Absolute time separation must be within 1 day
        max_interval = datetime.timedelta(days=1)
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

