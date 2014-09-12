"""
This module holds the CalibrationGMOS class
"""
import datetime

from fits_storage_config import using_sqlite
from orm.diskfile import DiskFile
from orm.header import Header
from orm.gmos import Gmos
from cal.calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationGMOS(Calibration):
    """
    This class implements a calibration manager for GMOS.
    It is a subclass of Calibration
    """
    gmos = None

    def __init__(self, session, header, descriptors, types):
        """
        This is the init method for the GMOS calibration subclass.
        """
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the gmosheader
        if header:
            query = session.query(Gmos).filter(Gmos.header_id == self.descriptors['header_id'])
            self.gmos = query.first()

        # Populate the descriptors dictionary for GMOS
        if self.from_descriptors:
            self.descriptors['disperser'] = self.gmos.disperser
            self.descriptors['filter_name'] = self.gmos.filter_name
            self.descriptors['focal_plane_mask'] = self.gmos.focal_plane_mask
            self.descriptors['detector_x_bin'] = self.gmos.detector_x_bin
            self.descriptors['detector_y_bin'] = self.gmos.detector_y_bin
            self.descriptors['amp_read_area'] = self.gmos.amp_read_area
            self.descriptors['read_speed_setting'] = self.gmos.read_speed_setting
            self.descriptors['gain_setting'] = self.gmos.gain_setting
            self.descriptors['nodandshuffle'] = self.gmos.nodandshuffle
            self.descriptors['nod_count'] = self.gmos.nod_count
            self.descriptors['nod_pixels'] = self.gmos.nod_pixels
            self.descriptors['prepared'] = self.gmos.prepared
            self.descriptors['overscan_trimmed'] = self.gmos.overscan_trimmed
            self.descriptors['overscan_subtracted'] = self.gmos.overscan_subtracted

        # Set the list of required calibrations
        self.set_required()

    def set_required(self):
        """
        This method determines which calibration types are required
        by the target data set, and records the list of required
        calibration types in the class required variable.
        """
        self.required = []

        if self.descriptors:

            # MASK files do not require anything,
            if self.descriptors['observation_type'] == 'MASK':
                return

            # For now, only RAW files get calibrations
            if self.descriptors['reduction'] != 'RAW':
                return

            # Do BIAS
            require_bias = True
            # BIASes do not require a bias.
            if self.descriptors['observation_type'] == 'BIAS':
                require_bias = False

            # Custom ROI acq images (efficient MOS acquisitions) don't require a BIAS.
            # As of Nov-2012, QAP doesn't send this descriptor (QAP Trac # 408), so ignore if not present
            if 'detector_roi_setting' in self.descriptors.keys():
                if self.descriptors['detector_roi_setting'] == 'Custom' and self.descriptors['observation_class'] == 'acq':
                    require_bias = False

                # Anything that's ROI = Central Stamp does not require a bias
                if self.descriptors['detector_roi_setting'] == 'Central Stamp':
                    require_bias = False

            if require_bias:
                self.required.append('bias')
                self.required.append('processed_bias')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it needs an arc
            if ((self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight')):
                self.required.append('arc')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it needs a flat and a processed_flat
            if ((self.descriptors['spectroscopy'] == True) and
                     (self.descriptors['observation_type'] == 'OBJECT') and
                     (self.descriptors['object'] != 'Twilight')):
                self.required.append('flat')
                self.required.append('processed_flat')


            # If it (is imaging) and
            # (is Imaging focal plane mask) and
            # (is an OBJECT) and (is not a Twilight)
            # then it needs a processed_flat
            if ((self.descriptors['spectroscopy'] == False) and
                     (self.descriptors['focal_plane_mask'] == 'Imaging') and
                     (self.descriptors['observation_type'] == 'OBJECT') and
                     (self.descriptors['object'] != 'Twilight')):

                self.required.append('processed_flat')

            # If it (is imaging) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it maybe needs a processed_fringe
            if ((self.descriptors['spectroscopy'] == False) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight')):
                self.required.append('processed_fringe')

            # If it (is nod and shuffle) and
            # (is an OBJECT), then it needs a dark
            if ((self.descriptors['nodandshuffle'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT')):
                self.required.append('dark')

            #self.required.append('flat')
            #self.required.append('processed_flat')
            #self.required.append('processed_fringe')


    def arc(self, processed=False, sameprog=False, many=None):
        """
        This method identifies the best GMOS ARC to use for the target
        dataset. The optional sameprog parameter is a boolean that says
        whether you require the result to be from the same science program.
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_ARC')
        else:
            query = query.filter(Header.observation_type == 'ARC').filter(Header.reduction == 'RAW')

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must Totally Match: Instrument, disperser
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.disperser == self.descriptors['disperser'])

        # Must match filter (from KR 20100423)
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])

        # Must Match central_wavelength
        #query = query.filter(func.abs(Header.central_wavelength-self.descriptors['central_wavelength']) < 0.001)
        # This might give better performance
        cenwlen_lo = float(self.descriptors['central_wavelength']) - 0.001
        cenwlen_hi = float(self.descriptors['central_wavelength']) + 0.001
        query = query.filter(Header.central_wavelength > cenwlen_lo).filter(Header.central_wavelength < cenwlen_hi)


        # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
        if self.descriptors['focal_plane_mask'] != '5.0arcsec':
            query = query.filter(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
        else:
            query = query.filter(Gmos.focal_plane_mask.like('%arcsec'))

        # Must match ccd binning
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])

        # The science amp_read_area must be equal or substring of the arc amp_read_area
        query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Should we insist on the program ID matching?
        if sameprog:
            query = query.filter(Header.program_id == self.descriptors['program_id'])

        # Absolute time separation must be within 1 year (31557600 seconds)
        # query = query.filter(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])) < 31557600)
        # Try this for performance
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

    def dark(self, many=None):
        """
        Method to find best GMOS Dark frame for the target dataset.
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
        query = query.filter(Header.observation_type == 'DARK')

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin,
        # read_speed_setting, gain_setting, exposure_time, nodandshuffle
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.read_speed_setting == self.descriptors['read_speed_setting'])
        query = query.filter(Gmos.gain_setting == self.descriptors['gain_setting'])

        # For some strange reason, GMOS exposure times sometimes come out
        # a few 10s of ms different between the darks and science frames

        # K.Roth 20110817 told PH just make it the nearest second, as you can't demand non integer times anyway.
        # Yeah, and GMOS-S ones come out a few 10s of *seconds* different - going to choose darks within 50 secs for now...
        # query = query.filter(func.abs(Header.exposure_time - self.descriptors['exposure_time']) < 50.0)
        # Better performance from a range than using abs
        exptime_lo = self.descriptors['exposure_time'] - 50.0
        exptime_hi = self.descriptors['exposure_time'] + 50.0
        query = query.filter(Header.exposure_time > exptime_lo).filter(Header.exposure_time < exptime_hi)

        query = query.filter(Gmos.nodandshuffle == self.descriptors['nodandshuffle'])
        if self.descriptors['nodandshuffle']:
            query = query.filter(Gmos.nod_count == self.descriptors['nod_count'])
            query = query.filter(Gmos.nod_pixels == self.descriptors['nod_pixels'])

        # The science amp_read_area must be equal or substring of the dark amp_read_area
        query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Absolute time separation must be within 1 year (31557600 seconds)
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

    def bias(self, processed=False, many=None):
        """
        Method to find the best bias frames for the target dataset
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
        query = query.filter(Header.observation_type == 'BIAS')
        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_BIAS')

         # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin, read_speed_setting, gain_setting
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.read_speed_setting == self.descriptors['read_speed_setting'])
        query = query.filter(Gmos.gain_setting == self.descriptors['gain_setting'])

        # The science amp_read_area must be equal or substring of the bias amp_read_area
        query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))


        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.

        # If the target frame is prepared, then we match the overscan state. If the target frame is
        # not prepared, then we don't know what thier procesing intentions are, so we
        # give them the default (which is trimmed and subtracted).
        if processed:
            if self.descriptors['prepared'] == True:
                query.filter(Gmos.overscan_trimmed == self.descriptors['overscan_trimmed'])
                query.filter(Gmos.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                query.filter(Gmos.overscan_trimmed == True)
                query.filter(Gmos.overscan_subtracted == True)

        # Absolute time separation must be within ~3 months
        max_interval = datetime.timedelta(days=90)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)


        # Order by absolute time separation.
        if using_sqlite:
            # Need to check that this simplistic approach actually works
            query = query.order_by(func.abs(Header.ut_datetime - self.descriptors['ut_datetime']))
        else:
            # Postgres at least seems to need this, as sqlalchemy func.abs(interval) is not defined
            query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

    def flat(self, processed=False, many=None):
        """
        Method to find the best GMOS FLAT fields for the target dataset
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
        else:
            query = query.filter(Header.reduction == 'RAW').filter(Header.observation_type == 'FLAT')

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])
        query = query.filter(Gmos.read_speed_setting == self.descriptors['read_speed_setting'])
        query = query.filter(Gmos.gain_setting == self.descriptors['gain_setting'])
        query = query.filter(Header.spectroscopy == self.descriptors['spectroscopy'])

        # Focal plane mask must match for imaging too... To avoid daytime thru-MOS mask imaging "flats"
        query = query.filter(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])

        if self.descriptors['spectroscopy']:
            query = query.filter(Gmos.disperser == self.descriptors['disperser'])
            # Central wavelength is in microns (by definition in the DB table).
            cenwlen_lo = float(self.descriptors['central_wavelength']) - 0.001
            cenwlen_hi = float(self.descriptors['central_wavelength']) + 0.001
            query = query.filter(Header.central_wavelength > cenwlen_lo).filter(Header.central_wavelength < cenwlen_hi)


        # The science amp_read_area must be equal or substring of the flat amp_read_area
        query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Absolute time separation must be within ~ 6 months
        max_interval = datetime.timedelta(days=180)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation.
        if using_sqlite:
            # This doesn't actually work. The difference always comes out as zero
            query = query.order_by(func.abs(Header.ut_datetime - self.descriptors['ut_datetime']))
        else:
            # Postgres needs the following:
            query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

    def processed_fringe(self, many=None):
        """
        Method to find the best processed_fringe frame for the target dataset.
        Note that the concept of a raw fringe frame is meaningless.
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
        query = query.filter(Header.reduction == 'PROCESSED_FRINGE')

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])

        # The science amp_read_area must be equal or substring of the flat amp_read_area
        query = query.filter(Gmos.amp_read_area.like('%'+self.descriptors['amp_read_area']+'%'))

        # Absolute time separation must be within 1 year
        max_interval = datetime.timedelta(days=365)
        datetime_lo = self.descriptors['ut_datetime'] - max_interval
        datetime_hi = self.descriptors['ut_datetime'] + max_interval
        query = query.filter(Header.ut_datetime > datetime_lo).filter(Header.ut_datetime < datetime_hi)

        # Order by absolute time separation
        query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

        # For now, we only want one result - the closest in time, unless otherwise indicated
        if many:
            query = query.limit(many)
            return query.all()
        else:
            return query.first()

