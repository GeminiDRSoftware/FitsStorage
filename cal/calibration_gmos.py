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

import math

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

        # Set the list of applicable calibrations
        self.set_applicable()

    def set_applicable(self):
        """
        This method determines which calibration types are applicable
        to the target data set, and records the list of applicable
        calibration types in the class applicable variable.
        """
        self.applicable = []

        if self.descriptors:

            # MASK files do not require anything,
            if self.descriptors['observation_type'] == 'MASK':
                return

            # Do BIAS. Most things require Biases.
            require_bias = True
            if self.descriptors['observation_type'] == 'BIAS':
                # BIASes do not require a bias.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Custom' and self.descriptors['observation_class'] == 'acq':
                # Custom ROI acq images (efficient MOS acquisitions) don't require a BIAS.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Central Stamp':
                # Anything that's ROI = Central Stamp does not require a bias
                require_bias = False

            if require_bias:
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it needs an arc and flat and spectwilight 
            if ((self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight')):
                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('spectwilight')


            # If it (is imaging) and
            # (is Imaging focal plane mask) and
            # (is an OBJECT) and (is not a Twilight)
            # then it needs flats or a processed_flat
            if ((self.descriptors['spectroscopy'] == False) and
                     (self.descriptors['focal_plane_mask'] == 'Imaging') and
                     (self.descriptors['observation_type'] == 'OBJECT') and
                     (self.descriptors['object'] != 'Twilight')):

                self.applicable.append('flat')
                self.applicable.append('processed_flat')

            # If it (is imaging) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it maybe needs a processed_fringe
            if ((self.descriptors['spectroscopy'] == False) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight')):
                self.applicable.append('processed_fringe')

            # If it (is nod and shuffle) and
            # (is an OBJECT), then it needs a dark
            if ((self.descriptors['nodandshuffle'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT')):
                self.applicable.append('dark')
                self.applicable.append('processed_dark')


    def arc(self, processed=False, sameprog=False, howmany=None):
        """
        This method identifies the best GMOS ARC to use for the target
        dataset. The optional sameprog parameter is a boolean that says
        whether you require the result to be from the same science program.
        """
        # No arcs for imaging
        if self.descriptors['spectroscopy'] == False:
            return []

        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        # Default 1 arc
        howmany = howmany if howmany else 1

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
        # This gives better performance than abs?
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

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Should we insist on the program ID matching?
        if sameprog:
            query = query.filter(Header.program_id == self.descriptors['program_id'])

        # Absolute time separation must be within 1 year
        # query = query.filter(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])) < 31557600)
        # Try this for performance
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

    def dark(self, processed=False, howmany=None):
        """
        Method to find best GMOS Dark frame for the target dataset.
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_DARK')
            # Default number of processed darks
            if howmany is None: howmany = 1
        else:
            query = query.filter(Header.observation_type == 'DARK').filter(Header.reduction == 'RAW')
            # Default number of raw darks
            if howmany is None: howmany = 15

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
        exptime_lo = float(self.descriptors['exposure_time']) - 50.0
        exptime_hi = float(self.descriptors['exposure_time']) + 50.0
        query = query.filter(Header.exposure_time > exptime_lo).filter(Header.exposure_time < exptime_hi)

        query = query.filter(Gmos.nodandshuffle == self.descriptors['nodandshuffle'])
        if self.descriptors['nodandshuffle']:
            query = query.filter(Gmos.nod_count == self.descriptors['nod_count'])
            query = query.filter(Gmos.nod_pixels == self.descriptors['nod_pixels'])

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))


        # Absolute time separation must be within 1 year (31557600 seconds)
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

    def bias(self, processed=False, howmany=None):
        """
        Method to find the best bias frames for the target dataset
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))
        query = query.filter(Header.observation_type == 'BIAS')
        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_BIAS')
            # Default number of processed Biases
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Default number of raw biases
            howmany = howmany if howmany else 50

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

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
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
        # query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
        # Use the ut_datetime_secs column for faster and more portable ordering
        targ_ut_dt_secs = int((self.descriptors['ut_datetime'] - Header.UT_DATETIME_SECS_EPOCH).total_seconds())
        query = query.order_by(func.abs(Header.ut_datetime_secs - targ_ut_dt_secs))

        query = query.limit(howmany)
        return query.all()

    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GMOS FLAT fields for the target dataset
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number of processed flats
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Set default number of raw flats later depending on if spectroscopy

        # Only spectroscopy flats are actually obstype flat for gmos
        # Imaging flats are twilight flats
        if self.descriptors['spectroscopy']:
            query = query.filter(Header.observation_type == 'FLAT')
            # Default number of spectroscopy flats
            howmany = howmany if howmany else 2
        else:
            # Twilight flats are dayCal OBJECT frames with target Twilight
            query = query.filter(Header.observation_class == 'dayCal').filter(Header.observation_type == 'OBJECT')
            query = query.filter(Header.object == 'Twilight')
            # Default number of raw twilight imaging flats
            howmany = howmany if howmany else 20


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

            # Spectroscopy flats also have to somewhat match telescope position for flexure, as follows
            # this is from FitsStorage TRAC #43 discussion with KR 20130425
            # QAP might not give us these for now. Remove this 'if' later when it does
            if 'elevation' in self.descriptors:
                if self.descriptors['focal_plane_mask'].startswith('IFU'):
                    # For IFU, elevation must we within 7.5 degrees
                    el_thresh = 7.5 # degrees
                    el_hi = float(self.descriptors['elevation']) + el_thresh
                    el_lo = float(self.descriptors['elevation']) - el_thresh
                    query = query.filter(Header.elevation > el_lo).filter(Header.elevation < el_hi)
                    # and crpa*cos(el) must be within 7.5 degrees, ie crpa must be within 7.5 / cos(el)
                    # when el=90, cos(el) = 0 and the range is infinite. Only check at lower elevations
                    if self.descriptors['elevation'] < 85:
                        crpa_thresh = 7.5 / math.cos(math.radians(self.descriptors['elevation']))
                        crpa_hi = float(self.descriptors['cass_rotator_pa']) + crpa_thresh
                        crpa_lo = float(self.descriptors['cass_rotator_pa']) - crpa_thresh
                        # Should deal with wrap properly here, but need to figure out what we get in the headers round the wrap
                        # simple case will be fine unless they did an unwrap between the science and the flat
                        query = query.filter(Header.cass_rotator_pa > crpa_lo).filter(Header.cass_rotator_pa < crpa_hi)
                else:
                    # MOS or LS case (spectroscopy byt not IFU)
                    if self.descriptors['central_wavelength'] > 0.55 or self.descriptors['disperser'].startswith('R150'):
                        # Elevation must be within 15 degrees
                        el_thresh = 15.0
                        el_hi = float(self.descriptors['elevation']) + el_thresh
                        el_lo = float(self.descriptors['elevation']) - el_thresh
                        query = query.filter(Header.elevation > el_lo).filter(Header.elevation < el_hi)
                        # And crpa*col(el) must be within 15 degrees
                        if self.descriptors['elevation'] < 85:
                            crpa_thresh = 7.5 / math.cos(math.radians(self.descriptors['elevation']))
                            crpa_hi = float(self.descriptors['cass_rotator_pa']) + crpa_thresh
                            crpa_lo = float(self.descriptors['cass_rotator_pa']) - crpa_thresh
                            # Should deal with wrap properly here, but need to figure out what we get in the headers round the wrap
                            # simple case will be fine unless they did an unwrap between the science and the flat
                            query = query.filter(Header.cass_rotator_pa > crpa_lo).filter(Header.cass_rotator_pa < crpa_hi)
                    else:
                        # In this case, no elevation or crpa constraints.
                        pass


        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))


        # Absolute time separation must be within ~ 6 months
        max_interval = datetime.timedelta(days=180)
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

    def processed_fringe(self, howmany=None):
        """
        Method to find the best processed_fringe frame for the target dataset.
        Note that the concept of a raw fringe frame is meaningless.
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        # Note that the concept of a raw fringe frame is not valid
        query = query.filter(Header.reduction == 'PROCESSED_FRINGE')

        # Default number to associate
        howmany = howmany if howmany else 1

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

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

    def spectwilight(self, processed=False, howmany=None):
        """
        Method to find the best spectwilight - ie spectroscopy twilight
        ie MOS / IFU / LS twilight
        """
        # We don't handle processed ones (yet)
        if processed:
            return []

        # Not valid for imaging
        if self.descriptors['spectroscopy'] == False:
            return []

        # Default number to associate
        howmany = howmany if howmany else 2

        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        query = query.filter(Header.reduction == 'RAW')

        # They are OBJECT dayCal frames with target twilight 
        query = query.filter(Header.observation_type == 'OBJECT').filter(Header.observation_class == 'dayCal')
        query = query.filter(Header.object == 'Twilight')

        # Must be spectroscopy
        query = query.filter(Header.spectroscopy == True)

        # Search only the canonical (latest) entries
        query = query.filter(DiskFile.canonical == True)

        # Knock out the FAILs
        query = query.filter(Header.qa_state != 'Fail')

        # Must totally match instrument, detector_x_bin, detector_y_bin, filter, disperser, focal plane mask
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])
        query = query.filter(Gmos.disperser == self.descriptors['disperser'])
        query = query.filter(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])

        # Must match central wavelength to within some tolerance. We don't do separate ones for dithers in wavelength?
        tolerance = 0.002 # microns. 20 Angstroms either way, from IJ 20141024
        cenwlen_lo = float(self.descriptors['central_wavelength']) - tolerance
        cenwlen_hi = float(self.descriptors['central_wavelength']) + tolerance
        query = query.filter(Header.central_wavelength > cenwlen_lo).filter(Header.central_wavelength < cenwlen_hi)

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

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

